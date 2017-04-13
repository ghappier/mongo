package db

import (
	"fmt"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	//"gopkg.in/mgo.v2-unstable"
)

var (
	m    *MgoSessionUtils
	once sync.Once
	URL  string = "mongodb://localhost:27017"
	//URL        string = "mongodb://192.168.1.121:27017"
	DB         string = "lizqdb"
	COLLECTION string = "hezjcol"
)

func GetInstance() *MgoSessionUtils {
	once.Do(func() {
		session, err := mgo.Dial(URL)
		if err != nil {
			fmt.Errorf("无法连接mongodb : ", err.Error())
		} else {
			session.SetSyncTimeout(5 * time.Minute)
			session.SetSocketTimeout(5 * time.Minute)
			session.SetPoolLimit(200)
		}

		m = &MgoSessionUtils{
			mgoSession: session,
		}
	})
	return m
}

type MgoSessionUtils struct {
	mgoSession *mgo.Session
}

func (p MgoSessionUtils) GetSession() *mgo.Session {
	if p.mgoSession == nil {
		session, err := mgo.Dial(URL)
		if err != nil {
			fmt.Errorf("无法连接mongodb : ", err.Error())
		} else {
			session.SetSyncTimeout(5 * time.Minute)
			session.SetSocketTimeout(5 * time.Minute)
			session.SetPoolLimit(200)
		}

		p.mgoSession = session
	}
	//最大连接池默认为4096
	return p.mgoSession.Clone()
}
