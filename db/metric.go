package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type Data struct {
	Date  time.Time `bson:"date" json:"date"`
	Value float64   `bson:"value" json:"value"`
}

type Timeseries struct {
	Hour int    `bson:"hour" json:"hour"`
	Data []Data `bson:"data" json:"data"`
}
type Metric struct {
	Date              time.Time         `bson:"date" json:"date"`
	MetricName        string            `bson:"metric_name" json:"metric_name"`
	HostName          string            `bson:"hostname" json:"hostname"`
	EnvLocation       string            `bson:"env_location" json:"env_location"`
	MetricTag         map[string]string `bson:"metric_tag" json:"metric_tag"`
	LastestValue      float64           `bson:"lastest_value" json:"lastest_value"`
	LastestUpdateDate time.Time         `bson:"lastest_update_date" json:"lastest_update_date"`
	Timeseries        []Timeseries      `bson:"timeseries" json:"timeseries"`
}

func (u *Metric) String() string {
	text, err := json.Marshal(*u)
	if err != nil {
		fmt.Println("error : ", err)
	}
	return string(text)
}

type MetricSlice []Metric

func (u MetricSlice) String() string {
	text, err := json.Marshal(u)
	if err != nil {
		fmt.Println("error : ", err)
	}
	return string(text)
}
