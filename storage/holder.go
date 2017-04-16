package storage

import (
	"sync"

	"github.com/prometheus/prometheus/storage/remote"
)

var (
	holder     *StorageHolder
	onceHolder sync.Once
)

type StorageHolder struct {
	storages []Storage
}

func (s *StorageHolder) Save(request *remote.WriteRequest) {
	for _, s := range s.storages {
		s.Save(request)
	}
}

func (s *StorageHolder) AddStorage(storage Storage) {
	s.storages = append(s.storages, storage)
}
func GetStorageHolderInstance() *StorageHolder {
	onceHolder.Do(func() {
		holder = &StorageHolder{storages: make([]Storage, 0)}
	})
	return holder
}
