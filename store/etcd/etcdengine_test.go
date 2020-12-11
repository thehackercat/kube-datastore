package etcd

import (
	"flag"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/thehackercat/kube-datastore/store"
)

var (
	certfile = "test/certs/etcd.crt"
	keyfile  = "test/certs/etcd.key"
	cafile   = "test/certs/ca.crt"
	eps      = []string{"10.22.19.154:4001", "10.24.215.208:4001", "10.25.95.162:4001"}
	key      = "/clusterA/pod/prealocated10"
	workdir  string
)

func TestMain(m *testing.M) {
	workdir = os.Getenv("KUBEFED_WORKDIR")
	_ = flag.Set("alsologtostderr", "true")
	_ = flag.Set("log_dir", workdir+"log")
	_ = flag.Set("v", "10")
	flag.Parse()

	ret := m.Run()
	glog.Flush()
	os.Exit(ret)
}

func AssertNil(t *testing.T, a interface{}) {
	if a != nil {
		t.Errorf("%v is not nil", a)
	}
}

func TestEtcdEngine_Get(t *testing.T) {
	etcdEngine, err := NewEtcdEngine(workdir+certfile, workdir+keyfile, workdir+cafile, eps)
	AssertNil(t, err)
	kvs, err := etcdEngine.Get(key)
	t.Logf("err: %v", err)
	t.Logf("kvs: %v", kvs)

	ver, err := etcdEngine.CAS(key, 0, []byte("1000"))
	t.Logf("err: %v", err)
	t.Logf("version: %d", ver)

	kvs, err = etcdEngine.Get(key)
	t.Logf("err: %v", err)
	t.Logf("kvs: %v", kvs)

	_, err = etcdEngine.CAS(key, 5, []byte("2000"))
	t.Logf("cas: %v", err)

	_, err = etcdEngine.CAS(key, 1, []byte("2000"))
	t.Logf("cas: %v", err)

	err = etcdEngine.Del(key)
	t.Logf("del: %v", err)
	etcdEngine.Close()
}

func onChange(params interface{}, typ store.OpType, key string, val []byte, ver int64) {
	glog.Infof("watch key(%s) value change, version: %d, val: %s\n", key, ver, string(val))
}

func TestEtcdEngine_Watcher(t *testing.T) {
	newkey := key + "/watchertest"
	etcdStore, err := NewEtcdEngine(workdir+certfile, workdir+keyfile, workdir+cafile, eps)
	AssertNil(t, err)
	etcdStore.RegisterWatcher(nil, newkey, onChange, store.StoreOpPut)
	var ver int64 = 0
	t.Logf("err: %v", err)
	t.Logf("version: %d", ver)
	for idx := 0; idx < 5; idx++ {
		val := idx * 1000
		curKey := newkey + "/" + strconv.Itoa(idx)
		ver, err = etcdStore.CAS(curKey, 0, []byte(strconv.Itoa(val)))
		glog.Infof("cas: %v", err)
		for subIdx := 0; subIdx < 5; subIdx++ {
			ver, err = etcdStore.CAS(curKey, ver, []byte(strconv.Itoa(val+subIdx*1000)))
			glog.Infof("cas: key: %s, err: %v\n", curKey, err)
		}
		time.Sleep(1 * time.Second)
	}
	time.Sleep(2 * time.Second)
	etcdStore.Close()
}

func TestLease(t *testing.T) {
	lease := key + "/lease1/" + time.Now().Format("2006-02-01+15:04:05.000")
	etcdStore, err := NewEtcdEngine(workdir+certfile, workdir+keyfile, workdir+cafile, eps)
	AssertNil(t, err)
	err = etcdStore.SetWithRelease(lease, []byte("lease"), 5*time.Second, true)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Second * 20)
	etcdStore.Close()
	// Output: ttl: 5
}
