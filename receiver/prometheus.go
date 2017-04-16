package receiver

import (
	"io/ioutil"
	"log"
	"net/http"

	//"github.com/ghappier/mongo/pool"
	"github.com/ghappier/mongo/storage"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/storage/remote"
)

/*
type PrometheusReceiver struct {
	producer *pool.Producer
}

func NewPrometheusReceiver(producer *pool.Producer) PrometheusReceiver {
	return PrometheusReceiver{producer: producer}
}
*/
//func (p PrometheusReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		//w.WriteHeader(http.StatusMethodNotAllowed)
		http.Error(w, "only post is allowed", http.StatusInternalServerError)
		return
	}
	regBuf, err := ioutil.ReadAll(snappy.NewReader(r.Body))
	if err != nil {
		log.Fatalf("read and uncompress request body error : %s\n", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var req remote.WriteRequest
	if err := proto.Unmarshal(regBuf, &req); err != nil {
		log.Fatalf("protobuf unmarshal error : %s\n", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	storage.GetStorageHolderInstance().Save(&req)
	//p.producer.Produce(pool.Job{WriteRequest: &req})
	w.WriteHeader(http.StatusOK)
	r.Body.Close()
}
