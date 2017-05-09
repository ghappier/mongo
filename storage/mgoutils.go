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
		go func() {
			for {
				select {
				case <-time.After(time.Minute * 1):
					if m.GetSession() == nil {
						s, _ := tryDial(mongodbConfig)
						m.mgoSession = s
					}
				}
			}
		}()
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
	/*
		var url string
		//mongodb://myuser:mypass@localhost:40001,otherhost:40001/mydb
		if mongodbConfig.Username == "" {
			url = fmt.Sprintf("mongodb://%s:%s@%s/%s", mongodbConfig.Username, mongodbConfig.Password, mongodbConfig.Url, mongodbConfig.Db)
		} else {
			url = fmt.Sprintf("mongodb://%s/%s", mongodbConfig.Url, mongodbConfig.Db)
		}
		session, err := mgo.Dial(url)
		if err == nil {
			session.SetSyncTimeout(1 * time.Minute)
			session.SetSocketTimeout(1 * time.Minute)
		}
		return session, err
	*/
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
