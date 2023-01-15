package sync

import (
	"fmt"
	"go-mysql-kafka/kafka"

	"github.com/pkg/errors"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-redis/redis/v8"
	"github.com/siddontang/go-log/log"
)

type CanalService struct {
	canal           *canal.Canal
	redisPosManager *RedisPosManager

	cfg *Config
}

// NewCanal 初始化canal 客户端
func NewCanal(config *Config) (*CanalService, error) {

	redisPosManager, err := prepareRedisPosManager(config)
	if err != nil {
		log.Errorf("Init redis position manager failed, err is %v", err)
		return nil, err
	}

	conf := config.DbBinlogConf
	cfg := canal.NewDefaultConfig()

	// 以下几个参数最关键，其他的参数参考defaultConfig 默认值
	cfg.ServerID = conf.Id
	cfg.User = conf.User
	cfg.Password = conf.Pwd
	cfg.Addr = conf.Host
	cfg.Flavor = conf.Flavor

	// 只进行增量消费，mysqlDump path 设置为空
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, err
	}

	go InitStatus(config.MetricConfig.Addr, config.MetricConfig.Path)

	return &CanalService{
		canal:           c,
		redisPosManager: redisPosManager,
		cfg:             config,
	}, nil
}

func (c *CanalService) Start() error {
	// init kafka Client
	kafkaClient, err := kafka.NewKafkaProducer(c.cfg.KafkaConfig)
	if err != nil {
		log.Errorf("Init redis position manager failed, err is %v", err)
		return err
	}

	// init eventHandler
	eventHandler := NewBinlogEvent2KafkaHandler(c, c.cfg, kafkaClient, c.redisPosManager)
	c.canal.SetEventHandler(eventHandler)

	return c.run()
}

func (c *CanalService) run() error {
	// 首先获取上次保存在redis 的位置
	pos, err := c.redisPosManager.Get(c.canal.Ctx())
	if err != nil || pos == nil {
		// 直接根据master 节点最新的gtid 进行消费
		masterGtidSet, err := c.canal.GetMasterGTIDSet()
		if err != nil {
			return err
		}
		err = c.canal.StartFromGTID(masterGtidSet)
		return err
	}

	// 以gtid 的方式启动，推荐方式
	if pos.GtidSet != nil {
		err = c.canal.StartFromGTID(pos.GtidSet)
		return err
	}
	// 从指定的binlog 日志文件启动
	// 这种方式无法支持多主架构模式
	if pos.Position != nil {
		err = c.canal.RunFrom(*pos.Position)
		return err
	}
	return errors.Errorf("Prepare position failed!!!")
}

func prepareRedisPosManager(cfg *Config) (*RedisPosManager, error) {
	redisCli, err := newRedisClient(cfg.RedisConfig)
	if err != nil {
		return nil, err
	}
	redisPosManager := NewRedisPosManager(redisCli, cfg.DbBinlogConf.BinlogPos)
	return redisPosManager, nil
}

func newRedisClient(config *RedisConfig) (*redis.Client, error) {
	redisCli := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", config.Host, config.Port),
		Password: config.Passwd,
		DB:       0, // use default DB
		// 连接池大小无需设置很大，固定情况下只有1个连接
		PoolSize: 5,
	})
	return redisCli, nil
}
