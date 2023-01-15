package sync

import (
	"context"
	"encoding/json"
	"time"

	"github.com/siddontang/go-log/log"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-redis/redis/v8"
)

// 为服务的高可用，通过redis 维护消费位移
// 目前每条消息执行时，均会同步进行保存位移

type RedisPosManager struct {
	redisCli     *redis.Client
	binlogConfig *binlogPosConfig
}

func NewRedisPosManager(redis *redis.Client, binlogConfig *binlogPosConfig) *RedisPosManager {
	return &RedisPosManager{
		redisCli:     redis,
		binlogConfig: binlogConfig,
	}
}

// SavePos 保存数据结构
type SavePos struct {
	Position *mysql.Position `json:"position"`
	GtidSet  mysql.GTIDSet   `json:"gtid"`
	SaveTime int64           `json:"saveTime"`
}

var lastSaveTime int64

const interval int64 = 3 * 1000

// MysqlBinlogPosKey 保存mysql binlog 消费位移 redis key
const MysqlBinlogPosKey = "mysql:binlog:pos"

func (r *RedisPosManager) Get(ctx context.Context) (*SavePos, error) {
	exist, _ := r.redisCli.Exists(ctx, MysqlBinlogPosKey).Result()
	if exist == 0 {
		return nil, nil
	}
	val, err := r.redisCli.Get(ctx, MysqlBinlogPosKey).Result()
	if err != nil {
		return nil, err
	}

	pos := &SavePos{}
	err = json.Unmarshal([]byte(val), pos)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func (r *RedisPosManager) savePosition(pos SavePos) error {
	if time.Now().UnixMilli() < lastSaveTime+interval {
		log.Debug("Failed to save position, because in interval time !")
		return nil
	}
	lastSaveTime = time.Now().UnixMilli()
	pos.SaveTime = lastSaveTime
	return r.Save(context.Background(), pos)
}

func (r *RedisPosManager) Save(ctx context.Context, pos SavePos) error {
	posVal, err := json.Marshal(pos)
	if err != nil {
		return err
	}

	_, err = r.redisCli.Do(ctx, "SET", MysqlBinlogPosKey, string(posVal)).Result()
	if err != nil {
		return err
	}
	return nil
}
