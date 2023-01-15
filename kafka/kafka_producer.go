package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/siddontang/go-log/log"
)

type Client struct {
	producer *kafka.Producer
}

type Config struct {
	Name     string `json:"name" mapstructure:"name"`
	Address  string `json:"address" mapstructure:"address"`
	Group    string `json:"group" mapstructure:"group"`
	Username string `json:"username" mapstructure:"username"`
	Passwd   string `json:"Passwd" mapstructure:"Passwd"`
	Protocol string `json:"protocol" mapstructure:"protocol"`
	Timeout  string `json:"timeout" mapstructure:"timeout"`
	// topic Config
	ChannelConfig *channelConfig `json:"channelConfig" mapstructure:"channel_config"`
}

// channelConfig 分区策略
type channelConfig struct {
	Strategy     string `json:"strategy" mapstructure:"strategy"`
	Topic        string `json:"topic" mapstructure:"topic"`
	PartitionNum uint16 `json:"partitionNum" mapstructure:"partition_num"`
	FieldName    string `json:"fieldName" mapstructure:"field_name"`
}

func NewKafkaProducer(cfg *Config) (*Client, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Address,
	})
	if err != nil {
		return nil, err
	}
	return &Client{producer: p}, nil
}

// SendToKafka 异步发送至Kafka
func (c *Client) SendToKafka(body []byte, topic, key string, partition int32) error {
	err := c.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
		},
		Value: body,
	}, nil)
	if err != nil {
		return err
	}
	log.Infof("send to kafka success, message is %s", string(body))
	return nil
}
