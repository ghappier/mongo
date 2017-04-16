package config

import (
	"testing"
)

func TestName(t *testing.T) {
	file := "mongo_config.xml"
	config, err := LoadFromZkConfig(file)
	if err != nil {
		t.Errorf("load config error\n")
	}
	if config.Port != ":9990" {
		t.Errorf("load config error, config.Port should be %s, but exact %s\n", ":9990", config.Port)
	}
	if config.OutputConfig.Mongodb.FlushInterval != 5 {
		t.Errorf("load config error, config.OutputConfig.Mongodb.FlushInterval should be %d, but exact %d\n", 5, config.OutputConfig.Mongodb.FlushInterval)
	}
}
