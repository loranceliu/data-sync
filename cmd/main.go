package main

import (
	config "data-sync/conf"
	"data-sync/core"
	"data-sync/handler"
	"data-sync/signal"
	"flag"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	"gopkg.in/yaml.v3"
	"os"
)

var release string

func init() {
	flag.StringVar(&release, "release", "local", "release model, optional local/dev/prod")
}

func main() {
	flag.Parse()

	s := fmt.Sprintf("etc/relation-%s.yaml", release)

	conf, err := loadConfig(s)
	if err != nil {
		log.Fatal(err)
	}

	c := replication.BinlogSyncerConfig{
		ServerID: conf.Slave.ServerID,
		Flavor:   conf.Slave.Flavor,
		Host:     conf.Mysql.Host,
		Port:     conf.Mysql.Port,
		User:     conf.Mysql.User,
		Password: conf.Mysql.Pwd,
	}

	binlogProcessor, err := core.NewBinlogProcessor(&c, conf.Slave.LoadPath, &handler.ElasticSearchHandler{})

	if err != nil {
		fmt.Println(err)
		return
	}

	signal.SignalHandle(binlogProcessor.Stop)

	binlogProcessor.StartSyncAndListen(&core.SyncPosition{
		Name:    conf.Slave.Binlog,
		Pos:     conf.Slave.Pos,
		LoadNew: conf.Slave.LoadNew,
	})

}

func loadConfig(filePath string) (*config.Config, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var conf config.Config
	err = yaml.Unmarshal(content, &conf)
	if err != nil {
		return nil, err
	}

	return &conf, nil
}
