package sync

import (
	"encoding/json"
	"go-mysql-kafka/kafka"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"

	"github.com/siddontang/go-log/log"

	"github.com/go-mysql-org/go-mysql/canal"
)

// 主要处理canal 事件的逻辑
// 实现接口canal.EventHandler

const (
	DEFAULT        = "default"
	PartitionByKey = "partition_by_key"
)

type BinlogEvent2KafkaHandler struct {
	canal.DummyEventHandler
	// kafka client 客户端
	kafkaClient     *kafka.Client
	canalService    *CanalService
	redisPosManager *RedisPosManager
	cfg             *Config
}

func NewBinlogEvent2KafkaHandler(canalService *CanalService, cfg *Config, kafkaClient *kafka.Client, redisPosManager *RedisPosManager) *BinlogEvent2KafkaHandler {
	return &BinlogEvent2KafkaHandler{
		canalService:    canalService,
		cfg:             cfg,
		kafkaClient:     kafkaClient,
		redisPosManager: redisPosManager,
	}
}

// OnRow 处理row event， 这里应该都是DML
// 解析并发送至Kafka 的格式
func (h *BinlogEvent2KafkaHandler) OnRow(e *canal.RowsEvent) error {
	var err error
	kafkaMsg, err := h.convertBinlogEvent2CanalFormat(e)
	if err != nil {
		// 暂时只记录错误日志，忽略这条消息，后续看如何更好的处理它
		log.Errorf("Failed to parse binlog event to canal format, err is %v", err)
		return nil
	}
	body, err := json.Marshal(kafkaMsg)
	if err != nil {
		log.Errorf("Serialize message failed, err is %v", err)
		return nil
	}

	channelCfg := h.cfg.KafkaConfig.ChannelConfig
	key, err := h.selectKey(kafkaMsg, channelCfg.FieldName)
	if err != nil {
		log.Errorf("Get message key failed, err is %v", err)
		return nil
	}

	partition, _ := h.selectPartition(channelCfg.Strategy, key, channelCfg.PartitionNum)

	err = h.kafkaClient.SendToKafka(body, channelCfg.Topic, key, partition)
	if err != nil {
		log.Errorf("Failed to send to kafka, err is %v", err)
		return nil
	}

	return nil
}

func (h *BinlogEvent2KafkaHandler) OnGTID(gtid mysql.GTIDSet) error {
	err := h.redisPosManager.savePosition(SavePos{
		GtidSet: gtid,
	})
	if err != nil {
		// 这里暂不做处理，本次失败后，可能下次就会成功
		log.Warnf("Save GTID set &v failed", gtid)
	}

	return nil
}

func (h *BinlogEvent2KafkaHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	err := h.redisPosManager.savePosition(SavePos{
		GtidSet:  set,
		Position: &pos,
	})
	if err != nil {
		// 这里暂不做处理，本次失败后，可能下次就会成功
		log.Warnf("Save position GTID set &v binlog position %v failed", set, pos)
	}
	return nil
}

func (h *BinlogEvent2KafkaHandler) String() string {
	return "BinlogEvent2KafkaHandler"
}

// selectPartition 根据分区策略选择分区号
func (h *BinlogEvent2KafkaHandler) selectPartition(strategy string, key string, partitionNum uint16) (int32, error) {
	switch strategy {
	case PartitionByKey:
		err := checkPartitionArgs(key, partitionNum)
		if err != nil {
			return 0, err
		}
		p, _ := kafka.PartitionByKey(key, partitionNum)
		return int32(p), nil
	default:
		p, _ := kafka.DefaultPartitionStrategy()
		return int32(p), nil
	}
}

func checkPartitionArgs(key string, partitionNum uint16) error {
	if key == "" || partitionNum <= 0 {
		return errors.Errorf("Partiotion paramters is illegal")
	}
	return nil
}

// selectKey 从数据中选择key
// 首先从data 数据中获取
func (h *BinlogEvent2KafkaHandler) selectKey(msg *Canal2KafkaMsg, fieldName string) (string, error) {
	if fieldName == "" {
		return "", nil
	}
	val, exist := msg.Data[fieldName]
	if exist {
		return strVal(val), nil
	}

	val, exist = msg.Old[fieldName]
	if exist {
		return strVal(val), nil
	}
	return "", nil
}
