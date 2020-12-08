package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/thehackercat/kube-datastore/store"
	"github.com/thehackercat/kube-datastore/util"

	"github.com/golang/glog"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
)

const (
	ETCD_OP_GET = "GET"
	ETCD_OP_CAS = "CAS"
)

type EtcdEngine struct {
	clt      *clientv3.Client
	stopChan chan struct{}
}

func NewEtcdEngine(cert, key, ca string, eps []string) (*EtcdEngine, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      cert,
		KeyFile:       key,
		TrustedCAFile: ca,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		glog.Errorf("init tls config failed, err: %v", err)
		return nil, err
	}
	config := clientv3.Config{
		Endpoints: eps,
		TLS:       tlsConfig,
	}
	client, err := clientv3.New(config)
	if err != nil {
		glog.Errorf("failed to init etcd client, err: %v", err)
		return nil, err
	}
	glog.V(1).Infof("etcd engine init success")
	return &EtcdEngine{
		clt:      client,
		stopChan: make(chan struct{}, 1),
	}, nil
}

func (ee *EtcdEngine) Close() {
	if ee.clt != nil {
		ee.clt.Close()
	}
	ee.stopChan <- struct{}{}
}

func (ee *EtcdEngine) Get(key string) (res []*store.KVPair, err error) {
	kv := clientv3.NewKV(ee.clt)
	getResp, err := kv.Get(context.TODO(), key, clientv3.WithPrefix())
	if err != nil {
		err = fmt.Errorf("etcd engine get key[%s] failed, err: %v", key, err)
		glog.Error(err)
		return nil, err
	}
	if len(getResp.Kvs) == 0 {
		return nil, store.ErrObjNotExist
	}
	res = make([]*store.KVPair, len(getResp.Kvs))
	for idx, kv := range getResp.Kvs {
		curKVP := &store.KVPair{
			Key:     string(kv.Key),
			Val:     kv.Value,
			Version: kv.Version,
		}
		res[idx] = curKVP
	}
	glog.V(5).Infof("etcd engine op [%s] with prefix[%s] finished, item cnt: %d", ETCD_OP_GET, key, len(getResp.Kvs))
	return res, nil
}

func (ee *EtcdEngine) initVal(key string, val []byte) (version int64, err error) {
	kv := clientv3.NewKV(ee.clt)
	txn := kv.Txn(context.TODO())
	txnResp, err := txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(val))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return -1, fmt.Errorf("failed to init '%s', internal error: %v", key, err)
	}
	if !txnResp.Succeeded {
		if len(txnResp.Responses[0].GetResponseRange().Kvs) > 0 {
			return txnResp.Responses[0].GetResponseRange().Kvs[0].Version,
				fmt.Errorf("key '%s' already exist, and version is %d", key, txnResp.Responses[0].GetResponseRange().Kvs[0].Version)
		} else {
			err = fmt.Errorf("'%s' init failed, resp: %v", key, txnResp)
			glog.Error(err)
		}
	}

	return 1, err
}

func (ee *EtcdEngine) CAS(key string, version int64, val []byte) (res int64, err error) {
	glog.V(5).Infof("etcd engine op [%s] call, version: %d, key: %s", ETCD_OP_CAS, version, key)
	if version == 0 {
		return ee.initVal(key, val)
	}
	kv := clientv3.NewKV(ee.clt)
	txn := kv.Txn(context.TODO())
	txnResp, err := txn.If(clientv3.Compare(clientv3.Version(key), "=", version)).
		Then(clientv3.OpPut(key, string(val))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		err = fmt.Errorf("failed to swap '%s', internal error: %v", key, err)
		return -1, err
	}
	if txnResp.Succeeded {
		glog.V(5).Infof("etcd engine op [%s], version: %d, key: %s", ETCD_OP_CAS, version+1, key)
		return version + 1, nil
	}
	if len(txnResp.Responses[0].GetResponseRange().Kvs) == 0 {
		return -1, fmt.Errorf("cas failed, key not exist")
	}
	return txnResp.Responses[0].GetResponseRange().Kvs[0].Version, fmt.Errorf("cas failed, version[%d] not matched, latest version is %d",
		version, txnResp.Responses[0].GetResponseRange().Kvs[0].Version)
}

func (ee *EtcdEngine) RegisterWatcher(params interface{}, key string, watcher store.ChangeWatcher, eventType store.OpType) {
	go ee.watch(params, key, watcher, eventType)
}

func (ee *EtcdEngine) watch(params interface{}, key string, watcher store.ChangeWatcher, eventType store.OpType) {
	rch := ee.clt.Watch(context.Background(), key, clientv3.WithPrefix())
	for {
		select {
		case wresp := <-rch:
			for _, ev := range wresp.Events {
				glog.V(5).Infof("type: %v, key: %s， version: %d", ev.Type, key, ev.Kv.Version)
				watcher(params, store.OpType(ev.Type), string(ev.Kv.Key), ev.Kv.Value, ev.Kv.Version)
			}
		case <-ee.stopChan:
			// !!! do not use break to jump out of for select, it can only jump out of select, not for loop;
			glog.V(5).Infof("watch goroutine exit")
			// return directly
			return
		}
	}
}

func (ee *EtcdEngine) Del(key string) error {
	kv := clientv3.NewKV(ee.clt)
	_, err := kv.Delete(context.Background(), key, clientv3.WithPrefix())
	return err
}

// ttl in seconds
func (ee *EtcdEngine) SetWithRelease(key string, val []byte, ttl time.Duration, keepAlive bool) (err error) {
	// minimum lease TTL is 5-second
	resp, err := ee.clt.Grant(context.TODO(), int64(ttl/time.Second))
	if err != nil {
		return err
	}

	// after 5 seconds, the key 'foo' will be removed
	kv := clientv3.NewKV(ee.clt)
	txn := kv.Txn(context.TODO())
	txnResp, err := txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(val), clientv3.WithLease(resp.ID))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		err = fmt.Errorf("failed to init '%s', internal error: %v", key, err)
		glog.Error(err)
		return err
	}
	if !txnResp.Succeeded {
		if len(txnResp.Responses[0].GetResponseRange().Kvs) > 0 {
			glog.Errorf("key '%s' already exist, and version is %d", key, txnResp.Responses[0].GetResponseRange().Kvs[0].Version)
			return util.ErrKeyLeaseLocked
		}
		err = fmt.Errorf("'%s' init failed, resp: %v", key, txnResp)
		glog.Error(err)
		return err
	}

	if keepAlive {
		//respCh, kaerr := ee.clt.KeepAlive(context.TODO(), resp.ID)
		_, kaerr := ee.clt.KeepAlive(context.TODO(), resp.ID)
		if kaerr != nil {
			glog.Fatal(kaerr)
		}
		//go func() {
		//	for {
		//		select {
		//		case resp := <-respCh:
		//			glog.V(5).Infof("key[%s] lease[%s] update, ttl: %d", key, resp.String(), resp.TTL)
		//		case <-ee.stopChan:
		//			return
		//		default:
		//			time.Sleep(500 * time.Millisecond)
		//		}
		//	}
		//}()
	}
	return err
}
