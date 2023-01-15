package main

import (
	"flag"
	"go-mysql-kafka/sync"
	"os"

	"github.com/siddontang/go-log/log"
	"github.com/spf13/viper"
)

// 服务启动
func main() {
	cfg, err := readConfig()
	if err != nil {
		os.Exit(1)
	}

	canalService, err := sync.NewCanal(cfg)
	if err != nil {
		log.Errorf("Init canal client failed, err is %v", err)
		os.Exit(1)
		return
	}

	err = canalService.Start()
	if err != nil {
		log.Errorf("Init canal client failed, err is %v", err)
		os.Exit(1)
		return
	}
	log.Info("Start Success !!!")
}

func readConfig() (*sync.Config, error) {
	var cfgFile string
	var err error
	v := viper.New()
	flag.StringVar(&cfgFile, "cfg", "application.yml", "Please enter config file cfgFile")
	flag.Parse()

	log.Infof("Load config file from %s", cfgFile)
	v.SetConfigFile(cfgFile)

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}
	cfg := &sync.Config{}
	err = v.Unmarshal(cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}
