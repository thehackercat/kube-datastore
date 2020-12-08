package store

import (
	"fmt"
	"time"
)

type ChangeWatcher func(interface{}, OpType, string, []byte, int64)

var (
	ErrObjNotExist = fmt.Errorf("obj not exist")
)

type OpType int32

const (
	StoreOpPut = 0
	StoreOpDel = 1
	StoreOpAll = 2
	OpRetry    = 3
)

// Store is a kv store with version control.
// VersionChangeWatcher is used to update version info
type KVPair struct {
	Key     string
	Val     []byte
	Version int64
}
type StoreEngine interface {
	Get(string) ([]*KVPair, error)
	Del(key string) error
	CAS(string, int64, []byte) (int64, error)
	RegisterWatcher(param interface{}, key string, watcher ChangeWatcher, op OpType)
	SetWithRelease(string, []byte, time.Duration, bool) error
	Close()
}
