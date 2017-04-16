package storage

import (
	"github.com/prometheus/prometheus/storage/remote"
)

type Storage interface {
	Save(request *remote.WriteRequest)
}
