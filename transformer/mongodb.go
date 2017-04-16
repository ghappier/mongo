package transformer

import (
	"sync"
	"time"

	"github.com/ghappier/mongo/model"
	"github.com/prometheus/prometheus/storage/remote"
)

var (
	transformer      *MongodbTransformer
	onceTransformer  sync.Once
	MetricNameLable  = "__name__"
	HostnameLabel    = "hostname"
	EnvLocationLabel = "env_location"
	MetricTagLabel   = "metric_tag"
)

type MongodbTransformer struct {
}

func (s *MongodbTransformer) Transform(request *remote.WriteRequest) []model.Metric {
	var ts []*remote.TimeSeries = request.GetTimeseries()
	var ms = make([]model.Metric, 0, len(ts))
	for _, v := range ts {
		var tag = make(map[string]string)
		var metric = model.Metric{}
		metric.MetricTag = tag
		var labels []*remote.LabelPair = v.GetLabels()
		for _, val := range labels {
			name := val.GetName()
			value := val.GetValue()
			switch name {
			case MetricNameLable:
				metric.MetricName = value
			case HostnameLabel:
				metric.HostName = value
			case EnvLocationLabel:
				metric.EnvLocation = value
			default:
				tag[name] = value
			}
		}

		var samples []*remote.Sample = v.GetSamples()
		mts := make([]model.Timeseries, 0, len(samples))
		for _, val := range samples {
			metric.LastestValue = val.GetValue()
			mdate := time.Unix(val.GetTimestampMs(), 0)
			year, month, day := mdate.Date()
			today := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
			metric.Date = today
			metric.LastestUpdateDate = mdate
			tts := model.Timeseries{}
			tts.Hour = mdate.Hour()
			data := make([]model.Data, 0, len(samples))
			firstData := model.Data{
				Date:  mdate,
				Value: val.GetValue(),
			}
			data = append(data, firstData)
			tts.Data = data
			mts = append(mts, tts)
		}
		metric.Timeseries = mts
		ms = append(ms, metric)
	}
	return ms
}

func GetTransformerInstance() *MongodbTransformer {
	onceTransformer.Do(func() {
		transformer = &MongodbTransformer{}
	})
	return transformer
}
