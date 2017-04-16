package config

import (
	"encoding/base64"
	"encoding/xml"
	"io/ioutil"
	"strconv"

	"github.com/golang/glog"
)

type Configs struct {
	Config []Config `xml:"config"`
}
type Config struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value"`
}

type PromReceiverConfig struct {
	MaxQueue     int          `json:"max_queue"`
	MaxWorker    int          `json:"max_worker"`
	Route        string       `json:"route"`
	Port         string       `json:"port"`
	InputConfig  InputConfig  `json:"input"`
	OutputConfig OutputConfig `json:"output"`
}
type InputConfig struct {
	Prometheus PrometheusConfig `json:"prometheus"`
	Kafka      KafkaConfig      `json:"kafka"`
}
type OutputConfig struct {
	Mongodb MongodbConfig `json:"mongodb"`
	Kafka   KafkaConfig   `json:"kafka"`
}
type PrometheusConfig struct {
	Enable bool `json:"enable"`
}
type KafkaConfig struct {
	Enable     bool   `json:"enable"`
	BrokerList string `json:"broker-list"`
	Topic      string `json:"topic"`
}
type MongodbConfig struct {
	Enable        bool   `json:"enable"`
	Url           string `json:"url"`
	Db            string `json:"db"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	PoolLimit     int    `json:"pool_limit"`
	Timeout       int    `json:"timeout"`
	TryTimes      int    `json:"try_times"`
	TryInterval   int    `json:"try_interval"`
	BulkSize      int    `json:"bulk_size"`
	FlushInterval int    `json:"flush_interval"`
}

func LoadFromZkConfig(file string) (*PromReceiverConfig, error) {
	var promReceiverConfig = PromReceiverConfig{}

	content, err := ioutil.ReadFile(file)
	if err != nil {
		//glog.Errorf("load zk config error: %s\n", err.Error())
		return &promReceiverConfig, err
	}
	var configs Configs
	err = xml.Unmarshal(content, &configs)
	if err != nil {
		//glog.Errorf("unmarshal zk config error: %s\n", err.Error())
		return &promReceiverConfig, err
	}

	for _, v := range configs.Config {
		switch v.Name {

		case "prom-receiver.max_queue":
			if v.Value == "" {
				promReceiverConfig.MaxQueue = 1000
			} else {
				if val, err := strconv.ParseInt(v.Value, 10, 0); err != nil {
					promReceiverConfig.MaxQueue = 1000
					glog.Errorf("prom-receiver.max_queue parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.MaxQueue = int(val)
				}
			}
		case "prom-receiver.max_worker":
			if v.Value == "" {
				promReceiverConfig.MaxWorker = 10
			} else {
				if val, err := strconv.ParseInt(v.Value, 10, 0); err != nil {
					promReceiverConfig.MaxWorker = 10
					glog.Errorf("prom-receiver.max_worker parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.MaxWorker = int(val)
				}
			}
		case "prom-receiver.route":
			if v.Value == "" {
				promReceiverConfig.Route = "/receive"
			} else {
				promReceiverConfig.Route = v.Value
			}
		case "prom-receiver.port":
			if v.Value == "" {
				promReceiverConfig.Port = ":9990"
			} else {
				promReceiverConfig.Port = v.Value
			}

		//input.prometheus
		case "prom-receiver.input.prometheus.enable":
			if v.Value == "" {
				promReceiverConfig.InputConfig.Prometheus.Enable = false
			} else {
				if val, err := strconv.ParseBool(v.Value); err != nil {
					promReceiverConfig.InputConfig.Prometheus.Enable = false
					glog.Errorf("prom-receiver.input.prometheus.enable parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.InputConfig.Prometheus.Enable = val
				}
			}

		//output.mongodb
		case "prom-receiver.output.mongodb.enable":
			if v.Value == "" {
				promReceiverConfig.OutputConfig.Mongodb.Enable = false
			} else {
				if val, err := strconv.ParseBool(v.Value); err != nil {
					promReceiverConfig.OutputConfig.Mongodb.Enable = false
					glog.Errorf("prom-receiver.output.mongodb.enable parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.OutputConfig.Mongodb.Enable = val
				}
			}
		case "mongodb.url":
			if v.Value == "" {
				glog.Errorf("mongodb.url is requred")
			}
			promReceiverConfig.OutputConfig.Mongodb.Url = v.Value
		case "mongodb.dbName":
			if v.Value == "" {
				glog.Errorf("mongodb.dbName is requred")
			}
			promReceiverConfig.OutputConfig.Mongodb.Db = v.Value
		case "mongodb.username":
			promReceiverConfig.OutputConfig.Mongodb.Username = v.Value
		case "mongodb.password":
			if v.Value == "" {
				promReceiverConfig.OutputConfig.Mongodb.Password = ""
			} else {
				if decodedPassword, err := base64.StdEncoding.DecodeString(v.Value); err != nil {
					promReceiverConfig.OutputConfig.Mongodb.Password = ""
					glog.Errorf("decode mongodb password error: %s\n", err.Error())
				} else {
					promReceiverConfig.OutputConfig.Mongodb.Password = string(decodedPassword)
				}
			}
		case "prom-receiver.output.mongodb.pool_limit":
			if v.Value == "" {
				promReceiverConfig.OutputConfig.Mongodb.PoolLimit = 100
			} else {
				if val, err := strconv.ParseInt(v.Value, 10, 0); err != nil {
					promReceiverConfig.OutputConfig.Mongodb.PoolLimit = 100
					glog.Errorf("prom-receiver.output.mongodb.pool_limit parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.OutputConfig.Mongodb.PoolLimit = int(val)
				}
			}
		case "prom-receiver.output.mongodb.timeout":
			if v.Value == "" {
				promReceiverConfig.OutputConfig.Mongodb.Timeout = 10
			} else {
				if val, err := strconv.ParseInt(v.Value, 10, 0); err != nil {
					promReceiverConfig.OutputConfig.Mongodb.Timeout = 10
					glog.Errorf("prom-receiver.output.mongodb.timeout parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.OutputConfig.Mongodb.Timeout = int(val)
				}
			}
		case "prom-receiver.output.mongodb.try_times":
			if v.Value == "" {
				promReceiverConfig.OutputConfig.Mongodb.TryTimes = 5
			} else {
				if val, err := strconv.ParseInt(v.Value, 10, 0); err != nil {
					promReceiverConfig.OutputConfig.Mongodb.TryTimes = 5
					glog.Errorf("prom-receiver.output.mongodb.try_times parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.OutputConfig.Mongodb.TryTimes = int(val)
				}
			}
		case "prom-receiver.output.mongodb.try_interval":
			if v.Value == "" {
				promReceiverConfig.OutputConfig.Mongodb.TryInterval = 10
			} else {
				if val, err := strconv.ParseInt(v.Value, 10, 0); err != nil {
					promReceiverConfig.OutputConfig.Mongodb.TryInterval = 10
					glog.Errorf("prom-receiver.output.mongodb.try_interval parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.OutputConfig.Mongodb.TryInterval = int(val)
				}
			}
		case "prom-receiver.output.mongodb.bulk_size":
			if v.Value == "" {
				promReceiverConfig.OutputConfig.Mongodb.BulkSize = 1000
			} else {
				if val, err := strconv.ParseInt(v.Value, 10, 0); err != nil {
					promReceiverConfig.OutputConfig.Mongodb.BulkSize = 1000
					glog.Errorf("prom-receiver.output.mongodb.bulk_size parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.OutputConfig.Mongodb.BulkSize = int(val)
				}
			}
		case "prom-receiver.output.mongodb.flush_interval":
			if v.Value == "" {
				promReceiverConfig.OutputConfig.Mongodb.FlushInterval = 5
			} else {
				if val, err := strconv.ParseInt(v.Value, 10, 0); err != nil {
					promReceiverConfig.OutputConfig.Mongodb.FlushInterval = 5
					glog.Errorf("prom-receiver.output.mongodb.flush_interval parse error : %s\n", err.Error())
				} else {
					promReceiverConfig.OutputConfig.Mongodb.FlushInterval = int(val)
				}
			}

		default:
		}

	}

	return &promReceiverConfig, nil
}
