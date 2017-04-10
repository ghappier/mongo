package db

import (
	"fmt"
	"strconv"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	//"gopkg.in/mgo.v2-unstable"
	//"gopkg.in/mgo.v2-unstable/bson"
	"github.com/patrickmn/go-cache"
)

var (
	MetricCache *cache.Cache = cache.New(5*time.Minute, 10*time.Minute)
)

type MetricDao struct {
	BulkSize      int
	FlushInterval uint32
	dataChannel   chan Metric
	count         int
}

func NewMetricDao(size int, interval uint32) *MetricDao {
	u := new(MetricDao)
	u.BulkSize = size
	u.FlushInterval = interval
	u.dataChannel = make(chan Metric, 1000000)
	go u.MetricDaoTimer()
	return u
}

func (u *MetricDao) Add(metric Metric) error {
	u.dataChannel <- metric
	if len(u.dataChannel) >= u.BulkSize {
		fmt.Println("批量保存")
		metrics := u.getData()
		return u.insert(metrics)
	}
	return nil
}
func (u *MetricDao) MetricDaoTimer() {
	timer1 := time.NewTicker(time.Duration(u.FlushInterval) * time.Second)
	for {
		select {
		case <-timer1.C:
			u.callback(nil)
		}
	}
}

func (u *MetricDao) callback(args interface{}) error {
	fmt.Println("超时自动保存")
	metrics := u.getData()
	return u.insert(metrics)
}

func (u *MetricDao) getData() *[]Metric {
	size := len(u.dataChannel)
	if size > u.BulkSize {
		size = u.BulkSize
	}
	metrics := make([]Metric, 0, size)
	for i := 0; i < size; i++ {
		select {
		case <-time.After(1 * time.Second):
			//fmt.Println("channel timeout 1 second")
			break
		case metric := <-u.dataChannel:
			metrics = append(metrics, metric)
		}
	}
	return &metrics
}

func (u *MetricDao) insert(metrics *[]Metric) error {
	size := len(*metrics)
	if size == 0 {
		fmt.Println("没有需要保存的数据")
		return nil
	}
	u.count = u.count + size
	fmt.Println("保存", size, "条数据，累计保存", u.count, "条数据")

	//session := u.getSession()
	session := GetInstance().GetSession()
	defer session.Close()
	c := session.DB(DB).C(COLLECTION)
	bulk := c.Bulk()
	bulkCount := 0
	for _, v := range *metrics {
		selector := bson.M{"date": v.Date, "metric_name": v.MetricName, "hostname": v.HostName}
		for k, v := range v.MetricTag {
			selector["metric_tag."+k] = v
		}

		if _, found := MetricCache.Get(v.MetricName); !found {
			MetricCache.Set(v.MetricName, true, cache.NoExpiration)

			updateSet := bson.M{"lastest_value": v.LastestValue, "lastest_update_date": v.LastestUpdateDate}
			timeSeries := make([]Timeseries, 0, 24)
			for i := 0; i < 24; i++ {
				ts := Timeseries{Hour: i, Data: make([]Data, 0, 0)}
				timeSeries = append(timeSeries, ts)
			}
			updateSetOnInsert := bson.M{"timeseries": timeSeries}
			bulk.Upsert(selector, bson.M{"$set": updateSet, "$setOnInsert": updateSetOnInsert})
			bulkCount += 1
			if bulkCount == 1000 {
				bulkCount = 0
				u.runBulk(bulk)
			}
		}

		for _, ts := range v.Timeseries {
			bulk.Update(selector, bson.M{"$set": bson.M{"lastest_value": v.LastestValue}, "$push": bson.M{"timeseries." + strconv.Itoa(ts.Hour) + ".data": bson.M{"$each": ts.Data}}})
			bulkCount += 1
			if bulkCount == 1000 {
				bulkCount = 0
				u.runBulk(bulk)
			}
		}
	}
	return u.runBulk(bulk)
}

func (u *MetricDao) runBulk(bulk *mgo.Bulk) error {
	result, err := bulk.Run()
	if err == nil {
		fmt.Println("bulk result :{Matched: ", result.Matched, ", Modified: ", result.Modified, "}")
	}
	return err
}

func (u *MetricDao) merge(metrics *[]Metric, met Metric) *[]Metric {
	if len(*metrics) < 1 {
		*metrics = append(*metrics, met)
		return metrics
	}
	match := false
	metricsSize := len(*metrics)
	for i := 0; i < metricsSize; i++ {
		v := (*metrics)[i]
		if u.keyEquals(&v, &met) {
			match = true
			mm := u.mergeMetric(&v, met)
			(*metrics)[i] = *mm
			break
		}
	}
	if !match {
		*metrics = append(*metrics, met)
	}
	return metrics
}

func (u *MetricDao) mergeMetric(metric *Metric, met Metric) *Metric {
	metric.LastestValue = met.LastestValue
	metric.LastestUpdateDate = met.LastestUpdateDate
	for _, vt := range met.Timeseries {
		for _, ts := range metric.Timeseries {
			if ts.Hour == vt.Hour {
				for _, vv := range vt.Data {
					ts.Data = append(ts.Data, vv)
				}
				break
			}
		}
	}
	return metric
}

func (u *MetricDao) keyEquals(m1 *Metric, m2 *Metric) bool {
	if !m1.Date.Equal(m2.Date) {
		return false
	}
	if m1.MetricName != m2.MetricName {
		return false
	}
	if m1.HostName != m2.HostName {
		return false
	}
	if m1.EnvLocation != m2.EnvLocation {
		return false
	}
	if !u.mapEquals(m1.MetricTag, m2.MetricTag) {
		return false
	}
	return true
}

func (u *MetricDao) mapEquals(m1 map[string]string, m2 map[string]string) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k, v := range m1 {
		if v != m2[k] {
			return false
		}
	}
	return true
}

/*
db.prom.update(
    {metric_name: "gc", hostname: "localhost"},
    {
        $set: {lastest_value: 300},
        $setOnInsert: {
            timeseries: [
                {hour: 0,data: []},
                {hour: 1,data: []},
                {hour: 2,data: []},
                {hour: 3,data: []},
                {hour: 4,data: []},
                {hour: 5,data: []},
                {hour: 6,data: []},
                {hour: 7,data: []},
                {hour: 8,data: []},
                {hour: 9,data: []},
                {hour: 10,data: []},
                {hour: 11,data: []},
                {hour: 12,data: []},
                {hour: 13,data: []},
                {hour: 14,data: []},
                {hour: 15,data: []},
                {hour: 16,data: []},
                {hour: 17,data: []},
                {hour: 18,data: []},
                {hour: 19,data: []},
                {hour: 20,data: []},
                {hour: 21,data: []},
                {hour: 22,data: []},
                {hour: 23,data: []}
            ]
        }
    },
    {upsert:true}
)


db.prom.update(
    {metric_name: "gc", hostname: "localhost"},
    {
        $push: {"timeseries.0.data":{name:"lizq",value:100}},
        $set: {lastest_value: 200}
    }
)

*/
