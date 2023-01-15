package sync

import (
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pkg/errors"
)

// Canal2KafkaMsg 消息体结构
// 各字段说明参考https://github.com/alibaba/canal/issues/3331
type Canal2KafkaMsg struct {
	// 数据发生的变化
	Data map[string]interface{} `json:"data"`
	Old  map[string]interface{} `json:"old"`

	Type     string `json:"type"`
	DataBase string `json:"database"`
	Table    string `json:"table"`
	Ddl      bool   `json:"isDdl"`

	// es是指mysql binlog里原始的时间戳，也就是数据原始变更的时间
	ExecuteTime uint64 `json:"es"`
	// ts是指canal收到这个binlog，构造为自己协议对象的时间
	Timestamp int64 `json:"ts"`

	// batch_id
	// The field is null now, maybe we will support its;
	Id uint32 `json:"id"`

	// 记录每一列数据类型在 MySQL 中的类型表示
	MysqlType map[string]interface{} `json:"mysqlType"`

	// 主键的字段名
	PkFiledNames []string `json:"pkNames"`
}

// convertBinlogEvent2CanalFormat 将*canal.RowsEvent binlog event 解析为canal 的数据结构
func (h *BinlogEvent2KafkaHandler) convertBinlogEvent2CanalFormat(e *canal.RowsEvent) (*Canal2KafkaMsg, error) {
	var err error
	columnNameArr, err := h.getColumnStr(e.Table)
	if err != nil {
		return nil, err
	}

	var old, data map[string]interface{}
	switch e.Action {
	case canal.InsertAction:
		data, err = h.parseRowData(columnNameArr, e.Rows[0])
		if err != nil {
			return nil, errors.Errorf("Parse row data failed, err is %+v", err)
		}
	case canal.UpdateAction:
		old, err = h.parseRowData(columnNameArr, e.Rows[0])
		if err != nil {
			return nil, errors.Errorf("Parse row data failed, err is %+v", err)
		}
		data, err = h.parseRowData(columnNameArr, e.Rows[1])
		if err != nil {
			return nil, errors.Errorf("Parse row data failed, err is %+v", err)
		}
	case canal.DeleteAction:
		old, err = h.parseRowData(columnNameArr, e.Rows[0])
		if err != nil {
			return nil, errors.Errorf("Parse row data failed, err is %+v", err)
		}
	}

	return &Canal2KafkaMsg{
		Data:         data,
		Old:          old,
		Type:         strings.ToUpper(e.Action),
		DataBase:     e.Table.Schema,
		Table:        e.Table.Name,
		Ddl:          false,
		ExecuteTime:  uint64(e.Header.Timestamp) * 1000,
		Timestamp:    time.Now().UnixMilli(),
		PkFiledNames: h.getPKNames(e.Table),
		MysqlType:    h.parseColumnMysqlType(e.Table),
	}, nil
}

// getColumnStr 获取列名数组
func (h *BinlogEvent2KafkaHandler) getColumnStr(table *schema.Table) ([]string, error) {
	if table == nil {
		return nil, errors.Errorf("Table info is null")
	}
	var columnStr []string
	for _, tableColumn := range table.Columns {
		columnStr = append(columnStr, tableColumn.Name)
	}
	return columnStr, nil
}

func (h *BinlogEvent2KafkaHandler) parseRowData(columnNameArr []string, rows []interface{}) (map[string]interface{}, error) {
	if len(columnNameArr) <= 0 || len(rows) <= 0 {
		return nil, errors.Errorf("Failed to parse row data, because args is illeagal")
	}

	// 理论上不会存在这种情况，因为canal 组件本身针对QueryEvent 会重新清理表结构缓存
	if len(columnNameArr) != len(rows) {
		return nil, errors.Errorf("Column number not equal row data number")
	}

	var dataMap = make(map[string]interface{}, len(rows))
	for index, data := range rows {
		columnName := columnNameArr[index]
		dataMap[columnName] = data
	}
	return dataMap, nil
}

func (h *BinlogEvent2KafkaHandler) getPKNames(table *schema.Table) []string {
	var pkColNames []string
	for _, index := range table.PKColumns {
		pkColNames = append(pkColNames, table.GetPKColumn(index).Name)
	}
	return pkColNames
}

func (h *BinlogEvent2KafkaHandler) parseColumnMysqlType(table *schema.Table) map[string]interface{} {
	if table == nil || len(table.Columns) <= 0 {
		return nil
	}
	var ans = make(map[string]interface{})
	for _, column := range table.Columns {
		ans[column.Name] = column.RawType
	}
	return ans
}
