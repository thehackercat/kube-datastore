# kube-datastore
KV storage engine wrapper for kubernetes.

Wrap following interfaces for interact with kubernetes store

- Get(string) ([]*KVPair, error)
- Del(key string) error
- CAS(string, int64, []byte) (int64, error)
- RegisterWatcher(param interface{}, key string, watcher ChangeWatcher, op OpType)
- SetWithRelease(string, []byte, time.Duration, bool) error
- Close()
## Usage

``` go
import github.com/thehackercat/kube-datastore

func main() {
    certfile, keyfile, cafile := "etcd-cert", "cted-cert-key", "ctcd-ca"
    endpoints := []string{"etcd-endpoint1", "etcd-endpoint2", "etcd-endpoint3"}

    etcdEngine, err := NewEtcdEngine(certfile, keyfile, cafile, endpoints)

    key := "test-key"
	kvs, err := etcdEngine.Get(key)

	ver, err := etcdEngine.CAS(key, 0, []byte("1000"))

	err = etcdEngine.Del(key)

	etcdEngine.Close()
}
```
