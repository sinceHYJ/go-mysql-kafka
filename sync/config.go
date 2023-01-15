package sync

import "go-mysql-kafka/kafka"

type Config struct {
	KafkaConfig  *kafka.Config `json:"kafka_conf" mapstructure:"kafka_conf"`
	DbBinlogConf *dbConfig     `json:"db_conf" mapstructure:"db_conf"`
	RedisConfig  *RedisConfig  `mapstructure:"redis_conf"`
	MetricConfig *MetricConfig `mapstructure:"metric_conf"`
}

type dbConfig struct {
	Id           uint32           `json:"id" mapstructure:"id"`
	Flavor       string           `json:"flavor" mapstructure:"flavor"`
	User         string           `json:"user" mapstructure:"user"`
	Pwd          string           `json:"pwd" mapstructure:"pwd"`
	Host         string           `json:"host" mapstructure:"host"`
	Port         uint16           `json:"port" mapstructure:"port"`
	Architecture string           `json:"architecture" mapstructure:"architecture"`
	BinlogPos    *binlogPosConfig `json:"binlog_pos" mapstructure:"binlog_pos"`
}

// binlogPosConfig 开始消费binlog的位置
type binlogPosConfig struct {
	// binlog 文件名
	Binlog string `json:"binlog" mapstructure:"binlog"`
	// offset 位移
	Position uint32 `json:"offset" mapstructure:"offset"`
}

type RedisConfig struct {
	Host   string `mapstructure:"host"`
	Port   string `mapstructure:"port"`
	Passwd string `mapstructure:"passwd"`
}

// MetricConfig metric 相关额配置
type MetricConfig struct {
	Addr string `mapstructure:"addr"`
	Path string `mapstructure:"path"`
}
