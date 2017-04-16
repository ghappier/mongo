package storage

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	//"fmt"
	//"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ghappier/mongo/config"
	"github.com/ghappier/mongo/model"
	"github.com/ghappier/mongo/transformer"
	"github.com/golang/glog"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/prometheus/storage/remote"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	MetricKeyCache *cache.Cache = cache.New(24*time.Hour, 10*time.Minute)
)

type MongodbStorage struct {
	mongodbConfig  *config.MongodbConfig
	count          int
	lock           *sync.RWMutex
	dataChannelMap map[string]chan model.Metric
	transf         *transformer.MongodbTransformer
}

func NewMongodbStorage(mongodbConfig *config.MongodbConfig) *MongodbStorage {
	u := new(MongodbStorage)
	u.mongodbConfig = mongodbConfig
	u.lock = new(sync.RWMutex)
	u.dataChannelMap = make(map[string]chan model.Metric)
	u.transf = transformer.GetTransformerInstance()
	go u.MongodbStorageTimer(mongodbConfig)
	return u
}

func (u *MongodbStorage) Save(request *remote.WriteRequest) {
	var metrics []model.Metric = u.transf.Transform(request)
	var mChanel chan model.Metric
	var present bool
	for _, metric := range metrics {
		collectionName := strings.Split(metric.MetricName, "_")[0]
		u.lock.RLock()
		if mChanel, present = u.dataChannelMap[collectionName]; !present {
			u.lock.RUnlock()
			u.lock.Lock()
			if mChanel, present = u.dataChannelMap[collectionName]; !present {
				mChanel = make(chan model.Metric, u.mongodbConfig.BulkSize)
				u.dataChannelMap[collectionName] = mChanel
			}
			u.lock.Unlock()
		} else {
			u.lock.RUnlock()
		}
		select {
		case <-time.After(time.Second * 10):
			glog.Errorf("写入channel超时，丢弃\n")
		case mChanel <- metric: //channel未满
			//fmt.Println("channel未满")
		default: //channel已满
			//fmt.Println("channel已满,批量保存")
			u.bulkSave(collectionName, mChanel)
			mChanel <- metric
		}
	}
}
func (u *MongodbStorage) MongodbStorageTimer(mongodbConfig *config.MongodbConfig) {
	timer := time.NewTicker(time.Duration(mongodbConfig.FlushInterval) * time.Second)
	for {
		select {
		case <-timer.C:
			//fmt.Println("超时自动保存")
			for k, v := range u.dataChannelMap {
				u.bulkSave(k, v)
			}
		}
	}
}

func (u *MongodbStorage) bulkSave(collection string, dataChannel chan model.Metric) {
	metrics := new([]model.Metric)
	//metrics := make([]model.Metric, 0, u.mongodbConfig.BulkSize)
LABEL_BREAK:
	for i := 0; i < u.mongodbConfig.BulkSize; i++ {
		select {
		case metric := <-dataChannel:
			//metrics = append(metrics, metric)
			metrics = u.merge(metrics, metric)
		default:
			break LABEL_BREAK
		}
	}
	//u.insert(collection, &metrics)
	u.insert(collection, metrics)
}

func (u *MongodbStorage) insert(collection string, metrics *[]model.Metric) error {
	size := len(*metrics)
	if size == 0 {
		//fmt.Println("没有需要保存的数据")
		return nil
	}
	u.count = u.count + size
	//fmt.Println("保存", size, "条数据，累计保存", u.count, "条数据")

	session := GetInstance(u.mongodbConfig).GetSession()
	if session == nil {
		glog.Error("数据库连接异常，无法批量保存")
		return errors.New("数据库连接异常，无法批量保存")
	}
	defer session.Close()
	c := session.DB(u.mongodbConfig.Db).C(collection)
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
			timeSeries := make([]model.Timeseries, 0, 24)
			for i := 0; i < 24; i++ {
				ts := model.Timeseries{Hour: i, Data: make([]model.Data, 0, 0)}
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

func (u *MongodbStorage) runBulk(bulk *mgo.Bulk) error {
	result, err := bulk.Run()
	if err == nil {
		glog.Infof("bulk result : {Matched: %d, Modified: %d}\n", result.Matched, result.Modified)
	} else {
		glog.Errorf("bulk error : %s\n", err.Error())
	}
	return err
}

func (u *MongodbStorage) merge(metrics *[]model.Metric, met model.Metric) *[]model.Metric {
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

func (u *MongodbStorage) mergeMetric(metric *model.Metric, met model.Metric) *model.Metric {
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

func (u *MongodbStorage) getMetricKey(m *model.Metric) string {
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

func (u *MongodbStorage) keyEquals(m1 *model.Metric, m2 *model.Metric) bool {
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

func (u *MongodbStorage) mapEquals(m1 map[string]string, m2 map[string]string) bool {
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
