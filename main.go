package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ghappier/mongo/db"
)

var (
	bulkSize      int    = 1000
	flushInterval uint32 = 30
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	Add()
}

var tmpl = `
{
    "date" : "%s",
    "hostname" : "%s",
    "metric_name" : "%s",
    "metric_tag" : {
        "aa" : "%s",
        "bb" : "%s",
        "instance" : "%s"
    },
    "lastest_value" : %f,
    "lastest_update_date" : "%s",
    "timeseries" : [ 
        {
            "hour" : %d,
            "data" : [{
				"date" : "%s",
				"value" : %f
            }]
        }
	]
}
`

func Add() {
	fmt.Println("开始执行")
	start := time.Now()
	metricDao := db.NewMetricDao(bulkSize, flushInterval)
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(metricDao *db.MetricDao, w *sync.WaitGroup) {
			for i := 0; i < 1000; i++ {
				now := time.Now()
				year, month, day := now.Date()
				today := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
				str := fmt.Sprintf(tmpl,
					today.Format(time.RFC3339),
					"localhost",
					"gc"+strconv.Itoa(rand.Intn(1)),
					"lizq",
					"hezj",
					"www.baidu.com",
					rand.Float64(),
					now.Format(time.RFC3339),
					now.Hour(),
					now.Format(time.RFC3339),
					rand.Float64())
				var metric db.Metric
				err := json.Unmarshal([]byte(str), &metric)
				if err != nil {
					//fmt.Errorf("反序列化错误：%s", err.Error())
					fmt.Printf("反序列化错误：%s\n", err.Error())
				}
				err = metricDao.Add(metric)
				if err != nil {
					//fmt.Errorf("保存mongodb错误：%s", err.Error())
					fmt.Printf("保存mongodb错误：%s\n", err.Error())
				}
			}
			w.Done()
		}(metricDao, &wg)
	}
	wg.Wait()
	end := time.Now()
	fmt.Println("完成,耗时：", end.Sub(start))
}
