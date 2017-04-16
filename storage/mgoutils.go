package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ghappier/mongo/config"
	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
)

var (
	m       *MgoUtils
	onceMgo sync.Once
)

func GetInstance(mongodbConfig *config.MongodbConfig) *MgoUtils {
	onceMgo.Do(func() {
		session, err := mydial(mongodbConfig)
		if err != nil {
			fmt.Errorf("无法连接mongodb : ", err.Error())
			session, err = tryDial(mongodbConfig)
		}

		m = &MgoUtils{
			mongodbConfig: mongodbConfig,
			mgoSession:    session,
		}
	})
	return m
}

type MgoUtils struct {
	mgoSession    *mgo.Session
	mongodbConfig *config.MongodbConfig
}

func (p MgoUtils) GetSession() *mgo.Session {
	if p.mgoSession == nil {
		return nil
	} else {
		return p.mgoSession.Clone()
	}
}

//连接mongodb
func mydial(mongodbConfig *config.MongodbConfig) (*mgo.Session, error) {
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:     strings.Split(mongodbConfig.Url, ","),
		Username:  mongodbConfig.Username,
		Password:  mongodbConfig.Password,
		Timeout:   time.Duration(mongodbConfig.Timeout) * time.Second,
		PoolLimit: mongodbConfig.PoolLimit,
		FailFast:  true,
	})
	if err == nil {
		session.SetSyncTimeout(1 * time.Minute)
		session.SetSocketTimeout(1 * time.Minute)
	}
	return session, err
}

//重连mongodb
func tryDial(mongodbConfig *config.MongodbConfig) (*mgo.Session, error) {
	for i := 0; i < mongodbConfig.TryTimes; i++ {
		time.Sleep(time.Duration(mongodbConfig.FlushInterval) * time.Second)
		glog.Infof("第%d次尝试重连...", i+1)
		session, err := mydial(mongodbConfig)
		if err == nil {
			glog.Infof("第%d次尝试重连成功", i+1)
			return session, err
		}
		glog.Errorf("第%d次尝试重连失败", i+1)
	}
	glog.Errorf("%d次尝试重连均失败", mongodbConfig.TryTimes)
	return nil, errors.New(strconv.Itoa(mongodbConfig.TryTimes) + "次尝试重连均失败")
}
