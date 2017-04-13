package pool

import (
	//"fmt"
	//"log"
	//"os"
	"github.com/ghappier/mongo/db"
)

// Job represents the job to be run
type Job struct {
	Metric db.Metric
}

// Worker represents the worker that executes the job
type Worker struct {
	workerPool chan chan Job
	jobChannel chan Job
	quit       chan bool
	metricDao  *db.MetricDao
}

func NewWorker(workerPool chan chan Job, dao *db.MetricDao) Worker {
	return Worker{
		workerPool: workerPool,
		jobChannel: make(chan Job, 1000),
		quit:       make(chan bool),
		metricDao:  dao,
	}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.workerPool <- w.jobChannel

			select {
			case job := <-w.jobChannel:
				/*
					// we have received a work request.
					if err := job.Payload.Save(); err != nil {
						//log.Errorf("Error uploading to S3: %s", err.Error())
						fmt.Errorf("Error uploading to S3: %s", err.Error())
					}
				*/
				w.metricDao.Add(job.Metric)
			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}
