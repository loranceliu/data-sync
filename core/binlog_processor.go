package core

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	"os"
	"strings"
	"sync"
)

type ActionType string

const (
	ActionTypeInsert ActionType = "INSERT"
	ActionTypeUpdate ActionType = "UPDATE"
	ActionTypeDelete ActionType = "DELETE"
)

type ResultJson struct {
	Schema string
	Table  string
	Type   ActionType
	Data   map[string]interface{}
}

var (
	i = 1
)

type BinlogProcessor struct {
	Syncer      *replication.BinlogSyncer
	SchemaCache *SchemaCache
	Handlers    []EventHandler
	BinChan     chan string
	Done        chan struct{}
	PosFileMux  sync.Mutex
	Binlog      string
	PosFile     string
}

func NewBinlogProcessor(config *replication.BinlogSyncerConfig, posDir string, handlers ...EventHandler) (*BinlogProcessor, error) {
	if len(posDir) == 0 {
		log.Fatalf("Pos directory can't be null")
	}

	var posFile string
	_, e1 := os.Stat(posDir)

	if e1 != nil {
		log.Fatalf("Pos directory '%s' does not exist.", posDir)
	} else {
		path := fmt.Sprintf("%s%s", posDir, "/rc.local")
		_, e2 := os.Stat(path)
		if e2 != nil {
			_, e3 := os.Create(path)
			if e3 != nil {
				log.Fatalf("Error creating file: %s", e3)
			}
		}
		posFile = path
	}

	schemaCache := &SchemaCache{}
	schemaCache.Init(config)
	syncer := replication.NewBinlogSyncer(*config)
	return &BinlogProcessor{Syncer: syncer, SchemaCache: schemaCache, Handlers: handlers, BinChan: make(chan string), Done: make(chan struct{}), PosFile: posFile}, nil
}

func (bp *BinlogProcessor) StartSyncAndListen(syncPosition *SyncPosition) {

	if syncPosition.LoadNew {
		bp.LoadPosFromDisk()
		syncPosition.Pos = 0
		syncPosition.Name = bp.Binlog
	}

	streamer, _ := bp.Syncer.StartSync(mysql.Position{
		Name: syncPosition.Name,
		Pos:  syncPosition.Pos,
	})

	go bp.LoadPos()

	for {
		select {
		case <-bp.Done:
			log.Info("system is Closing")
			return
		default:
			ev, _ := streamer.GetEvent(context.Background())
			bp.eventHandle(ev)
		}
	}

}

func (bp *BinlogProcessor) LoadPos() {
	for {
		select {
		case binLog := <-bp.BinChan:

			if i > 1 {

				bp.UpdatePos(binLog)

				bp.SavePosToDisk()
			}

			i++

		case <-bp.Done:
			log.Info("loader is closing")
			bp.Syncer.Close()
		}
	}
}

func (bp *BinlogProcessor) LoadPosFromDisk() {
	bp.PosFileMux.Lock()
	defer bp.PosFileMux.Unlock()

	// 从文件读取 pos
	content, err := os.ReadFile(bp.PosFile)
	if err == nil {
		// 将文件内容按行分割
		lines := strings.Split(strings.TrimSpace(string(content)), "\n")

		// 解析最后一行的数据
		if len(lines) > 0 {
			binLog := lines[len(lines)-1]
			if len(binLog) != 0 {
				bp.Binlog = binLog
			}
		}
	}
}

func (bp *BinlogProcessor) SavePosToDisk() {
	bp.PosFileMux.Lock()
	defer bp.PosFileMux.Unlock()

	// 以读写模式打开文件
	file, err := os.OpenFile(bp.PosFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
		return
	}
	defer file.Close()

	// 读取文件内容
	content, err := os.ReadFile(bp.PosFile)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
		return
	}

	// 将文件内容按行分割
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")

	// 保留最新的两行数据
	lines = append(lines, fmt.Sprintf("%s", bp.Binlog))
	if len(lines) > 2 {
		// 删除第一行
		lines = lines[1:]
	}

	// 将最新的两行数据写回文件
	_, err = file.WriteString(strings.Join(lines, "\n") + "\n")
	if err != nil {
		log.Fatalf("Error writing pos to file: %v", err)
	}
}

func (bp *BinlogProcessor) UpdatePos(binLog string) {
	// 更新 pos
	bp.Binlog = binLog
}

func (bp *BinlogProcessor) eventHandle(ev *replication.BinlogEvent) {
	if ev == nil || ev.Event == nil {
		return
	}
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		bp.parseEvent(ev, ActionTypeInsert)
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		bp.parseEvent(ev, ActionTypeUpdate)
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		bp.parseEvent(ev, ActionTypeDelete)
	case replication.ROTATE_EVENT:
		rotateEvent := ev.Event.(*replication.RotateEvent)
		if ev.Header.LogPos == 0 {
			bp.Binlog = string(rotateEvent.NextLogName)
			bp.BinChan <- bp.Binlog
		}
	default:
	}
}

func (bp *BinlogProcessor) parseEvent(rowEv *replication.BinlogEvent, actionType ActionType) {

	rowsEvent := rowEv.Event.(*replication.RowsEvent)
	schema, _ := bp.SchemaCache.getColumns(string(rowsEvent.Table.Schema), string(rowsEvent.Table.Table))

	row := make([]interface{}, 0, 0)

	if actionType == ActionTypeUpdate {
		row = rowsEvent.Rows[len(rowsEvent.Rows)-1]
	} else {
		row = rowsEvent.Rows[0]
	}

	rst := &ResultJson{}
	rst.Schema = schema.Database
	rst.Table = schema.Table
	rst.Type = actionType

	data := make(map[string]interface{})

	for index, c := range schema.Columns {
		v := row[index]
		if v == nil {
			v = "null"
		}
		data[c] = v
	}

	rst.Data = data

	for _, h := range bp.Handlers {
		h.Handle(rst)
	}

}

func (bp *BinlogProcessor) Stop() {
	close(bp.Done)
}
