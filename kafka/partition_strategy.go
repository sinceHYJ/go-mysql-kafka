package kafka

import (
	"hash/crc32"

	"github.com/pkg/errors"
)

// kafka 分区策略
// 默认
// 1. 默认分区0
// 2. 按照message key 进行分区

type PartitionStrategy interface {
	GetPartitionNo() (uint16, error)
}

// DefaultPartitionStrategy 默认分区策略，固定防止在0 分区中
func DefaultPartitionStrategy() (uint16, error) {
	return 0, nil
}

// PartitionByKey 按照Kafka Message key 进行分区
// 例如：key 可以选择某表的某字段，比如按照租户id，人员id 进行分区
func PartitionByKey(key string, partitionNum uint16) (uint16, error) {
	if partitionNum == 0 {
		return 0, errors.Errorf("Partition sum is zero")
	}
	if "" == key {
		return DefaultPartitionStrategy()
	}

	return hashcode(key) % partitionNum, nil
}

func hashcode(str string) uint16 {
	return uint16(crc32.ChecksumIEEE([]byte(str)))
}
