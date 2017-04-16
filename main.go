package main

import (
	"encoding/json"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/ghappier/mongo/config"
	//"github.com/ghappier/mongo/pool"
	"github.com/ghappier/mongo/receiver"
	"github.com/ghappier/mongo/storage"
	"github.com/golang/glog"
)

var (
	configFile = flag.String("config-file", "D:\\GO\\dev\\src\\github.com\\ghappier\\mongo\\config\\mongo_config.xml", "配置文件")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	go flushGlog()
	glog.Info("starting ...")

	if *configFile == "" {
		flag.PrintDefaults()
	}

	promConfig, err := config.LoadFromZkConfig(*configFile)
	if err != nil {
		glog.Errorf("load config file error: %s\n", err.Error())
		os.Exit(1)
	} else {
		if mconfig, err := json.Marshal(promConfig); err != nil {
			glog.Errorf("marshal error : %s\n", err.Error())
		} else {
			glog.Infof("prom-receiver config = \n%s\n", string(mconfig))
		}
	}

	storageHolder := storage.GetStorageHolderInstance()

	if promConfig.InputConfig.Prometheus.Enable {
		glog.Infoln("input prometheus is enabled")
	}
	if promConfig.InputConfig.Kafka.Enable {
		glog.Infoln("input kafka is enabled")
	}
	if promConfig.OutputConfig.Mongodb.Enable {
		glog.Infoln("output mongodb is enabled")
		mongodbStorage := storage.NewMongodbStorage(&promConfig.OutputConfig.Mongodb)
		storageHolder.AddStorage(mongodbStorage)
	}
	if promConfig.OutputConfig.Kafka.Enable {
		glog.Infoln("output kafka is enabled")
	}
	/*
		jobQueue := make(chan pool.Job, promConfig.MaxQueue)
		dispatcher := pool.NewDispatcher(jobQueue, promConfig.MaxWorker, storageHolder)
		dispatcher.Run()
		producer := pool.NewProducer(jobQueue)
		prometheusReceiver := receiver.NewPrometheusReceiver(producer)
		http.Handle(promConfig.Route, prometheusReceiver)
	*/
	http.HandleFunc("/receive", receiver.ServeHTTP) //设置访问的路由
	glog.Info("start complete")
	err = http.ListenAndServe(promConfig.Port, nil) //设置监听的端口
	if err != nil {
		glog.Errorf("ListenAndServe error : %s\n", err.Error())
	}
}

func flushGlog() {
	timer := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-timer.C:
			glog.Flush()
		}
	}
}
