package db

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	//"gopkg.in/mgo.v2-unstable"
	//"gopkg.in/mgo.v2-unstable/bson"
	"github.com/patrickmn/go-cache"
)

var (
	MetricKeyCache *cache.Cache = cache.New(24*time.Hour, 10*time.Minute)
)

type MetricDao struct {
	bulkSize       int
	flushInterval  uint32
	count          int
	lock           *sync.RWMutex
	dataChannelMap map[string]chan Metric
}

func NewMetricDao(size int, interval int) *MetricDao {
	u := new(MetricDao)
	u.bulkSize = size
	u.flushInterval = uint32(interval)
	u.lock = new(sync.RWMutex)
	u.dataChannelMap = make(map[string]chan Metric)
	go u.MetricDaoTimer()
	return u
}

func (u *MetricDao) Add(metric Metric) {
	var mChanel chan Metric
	var present bool
	u.lock.RLock()
	if mChanel, present = u.dataChannelMap[metric.MetricName]; !present {
		u.lock.RUnlock()
		u.lock.Lock()
		if mChanel, present = u.dataChannelMap[metric.MetricName]; !present {
			mChanel = make(chan Metric, 2*u.bulkSize)
			u.dataChannelMap[metric.MetricName] = mChanel
		}
		u.lock.Unlock()
	} else {
		u.lock.RUnlock()
	}
	select {
	case mChanel <- metric: //channel未满
		//fmt.Println("channel未满")
	default: //channel已满
		//fmt.Println("channel已满,批量保存")
		u.bulkSave(metric.MetricName, mChanel)
		mChanel <- metric
	}
}
func (u *MetricDao) MetricDaoTimer() {
	timer1 := time.NewTicker(time.Duration(u.flushInterval) * time.Second)
	for {
		select {
		case <-timer1.C:
			//fmt.Println("超时自动保存")
			for k, v := range u.dataChannelMap {
				u.bulkSave(k, v)
			}
		}
	}
}

func (u *MetricDao) bulkSave(collection string, dataChannel chan Metric) {
	metrics := new([]Metric)
LABEL_BREAK:
	for i := 0; i < u.bulkSize; i++ {
		select {
		case metric := <-dataChannel:
			//metrics = append(metrics, metric)
			metrics = u.merge(metrics, metric)
		default:
			break LABEL_BREAK
		}
	}
	u.insert(collection, metrics)
}

func (u *MetricDao) insert(collection string, metrics *[]Metric) error {
	size := len(*metrics)
	if size == 0 {
		//fmt.Println("没有需要保存的数据")
		return nil
	}
	u.count = u.count + size
	//fmt.Println("保存", size, "条数据，累计保存", u.count, "条数据")

	session := GetInstance().GetSession()
	defer session.Close()
	c := session.DB(DB).C(collection)
	bulk := c.Bulk()
	bulkCount := 0
	for _, v := range *metrics {
		selector := bson.M{"date": v.Date, "metric_name": v.MetricName, "hostname": v.HostName}
		for k, v := range v.MetricTag {
			selector["metric_tag."+k] = v
		}
		metricKey := u.getMetricKey(&v)
		if _, found := MetricKeyCache.Get(metricKey); !found {
			MetricKeyCache.Set(metricKey, true, cache.NoExpiration)

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
				bulk = c.Bulk()
			}

		}

		for _, ts := range v.Timeseries {
			bulk.Update(selector, bson.M{"$set": bson.M{"lastest_value": v.LastestValue}, "$push": bson.M{"timeseries." + strconv.Itoa(ts.Hour) + ".data": bson.M{"$each": ts.Data}}})

			bulkCount += 1
			if bulkCount == 1000 {
				bulkCount = 0
				u.runBulk(bulk)
				bulk = c.Bulk()
			}

		}
	}
	return u.runBulk(bulk)
}

func (u *MetricDao) runBulk(bulk *mgo.Bulk) error {
	result, err := bulk.Run()
	if err == nil {
		fmt.Println("bulk result :{Matched: ", result.Matched, ", Modified: ", result.Modified, "}")
	} else {
		fmt.Println(err.Error())
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

func (u *MetricDao) getMetricKey(m *Metric) string {
	s := m.Date.String() + m.MetricName + m.HostName + m.EnvLocation
	tags := make([]string, 0)
	for tag, _ := range m.MetricTag {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	for _, v := range tags {
		s += v
		s += m.MetricTag[v]
	}
	signByte := []byte(s)
	hash := md5.New()
	hash.Write(signByte)
	return hex.EncodeToString(hash.Sum(nil))
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
