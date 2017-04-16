package pool

import (
	//"fmt"
	//"log"
	//"os"
	"github.com/ghappier/mongo/storage"
	"github.com/prometheus/prometheus/storage/remote"
)

// Job represents the job to be run
type Job struct {
	WriteRequest *remote.WriteRequest
}

// Worker represents the worker that executes the job
type Worker struct {
	workerPool    chan chan Job
	jobChannel    chan Job
	quit          chan bool
	storageHolder *storage.StorageHolder
}

func NewWorker(workerPool chan chan Job, storageHolder *storage.StorageHolder) Worker {
	return Worker{
		workerPool:    workerPool,
		jobChannel:    make(chan Job, 100),
		quit:          make(chan bool),
		storageHolder: storageHolder,
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
				w.storageHolder.Save(job.WriteRequest)
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
