# Go-Mysql-Kafka

该服务模拟slave 监听binlog 并发送至Kafka， 类似于阿里巴巴的Canal 组件。
项目依赖于开源项目[go-mysql](https://github.com/go-mysql-org/go-mysql) 构建。

## DB 配置
建议开启GTID 模式，GTID的方式当然也适合多主架构。
```shell
gtid_on=1
log-bin=mysql-bin #添加这一行就ok  
binlog-format=ROW #选择row模式  
server_id=1 #配置mysql replaction需要定义，不能和canal的slaveId重复  
```
若采用普通的binlog 的方式进行消费，无法适配多主、主从模式下，因为同一个事件在不同节点的binlog offset 是不一样的。

目前尚不支持Galera Cluster 架构，会在后续版本支持。

## 位移的保存
目前使用Redis 的方式临时保存。为防止处理速度过快对redis 造成压力，采用每隔3s 的方式保存一次。
当然在服务的重启后，会存在重复的消费，下游的系统需要保持幂等性。


