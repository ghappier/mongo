package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ghappier/mongo/db"
	"github.com/ghappier/mongo/pool"
)

var (
	MaxWorker     = flag.Int("max-worker", 1, "工作线程数")
	MaxQueue      = flag.Int("max-queue", 100, "channel缓存大小")
	BulkSize      = flag.Int("bulk-size", 100, "批量提交大小")
	FlushInterval = flag.Int("flush-interval", 5, "自动刷新间隔")
	HostNames     = [...]string{
		"aa01",
		"aa02",
		"aa03",
		"aa04",
		"aa05",
		"aa06",
		"aa07",
		"aa08",
		"aa09",
		"aa10",
	}
	MetricNames = [...]string{
		"xx01",
		"xx02",
		"xx03",
		"xx04",
		"xx05",
		"xx06",
		"xx07",
		"xx08",
		"xx09",
		"xx10",
	}
	Tmpl = `
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
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	jobQueue := make(chan pool.Job, *MaxQueue)

	metricDao := db.NewMetricDao(*BulkSize, *FlushInterval)
	dispatcher := pool.NewDispatcher(jobQueue, *MaxWorker, metricDao)
	dispatcher.Run()
	producer := pool.NewProducer(jobQueue)

	add(producer)
	timer := time.NewTimer(time.Minute * 1)
	for i := 0; i < 10; i++ {
		select {
		case <-timer.C:
			timer.Reset(time.Minute * 1)
			add(producer)
		}
	}

	http.HandleFunc("/mmongo", func(w http.ResponseWriter, r *http.Request) {}) //设置访问的路由
	err := http.ListenAndServe(":9090", nil)                                    //设置监听的端口
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func add(producer *pool.Producer) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fmt.Println("开始执行")
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		//fmt.Println("开始发送第", i+1, "批数据")
		go func(p *pool.Producer, w *sync.WaitGroup) {
			for j := 0; j < 10000; j++ {
				now := time.Now()
				year, month, day := now.Date()
				today := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
				str := fmt.Sprintf(Tmpl,
					today.Format(time.RFC3339),
					HostNames[r.Intn(len(HostNames))],
					MetricNames[r.Intn(len(MetricNames))],
					"lizq_"+strconv.Itoa(r.Intn(100)),
					"hezj_"+strconv.Itoa(r.Intn(100)),
					"www.baidu.com/"+strconv.Itoa(r.Intn(100)),
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
				//fmt.Println("发送第", i+1, "批第", j+1, "条数据")
				p.Produce(pool.Job{Metric: metric})
			}
			w.Done()
		}(producer, &wg)
	}
	wg.Wait()
	end := time.Now()
	fmt.Println("完成,耗时：", end.Sub(start))
}
