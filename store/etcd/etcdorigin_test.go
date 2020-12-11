package etcd

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
)

func TestEtcdLease(t *testing.T) {
	tlsInfo := transport.TLSInfo{
		CertFile:      workdir + certfile,
		KeyFile:       workdir + keyfile,
		TrustedCAFile: workdir + cafile,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		t.Fatalf("init tls config failed, err: %v", err)
	}
	config := clientv3.Config{
		Endpoints: eps,
		TLS:       tlsConfig,
	}
	client, err := clientv3.New(config)
	if err != nil {
		t.Fatalf("failed to init etcd client, err: %v", err)
	}
	defer client.Close()

	// minimum lease TTL is 5-second
	resp, err := client.Grant(context.TODO(), 15)
	if err != nil {
		t.Fatal(err)
	}

	// after 5 seconds, the key 'foo' will be removed
	_, err = client.Put(context.TODO(), "foo", "bar", clientv3.WithLease(resp.ID))
	if err != nil {
		t.Fatal(err)
	}
}

func TestEtcdTxnLease(t *testing.T) {
	tlsInfo := transport.TLSInfo{
		CertFile:      workdir + certfile,
		KeyFile:       workdir + keyfile,
		TrustedCAFile: workdir + cafile,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		t.Fatalf("init tls config failed, err: %v", err)
	}
	config := clientv3.Config{
		Endpoints: eps,
		TLS:       tlsConfig,
	}
	client, err := clientv3.New(config)
	if err != nil {
		t.Fatalf("failed to init etcd client, err: %v", err)
	}
	defer client.Close()

	// minimum lease TTL is 5-second
	resp, err := client.Grant(context.TODO(), 5)
	if err != nil {
		t.Fatal(err)
	}

	// after 5 seconds, the key 'foo' will be removed
	kv := clientv3.NewKV(client)
	txn := kv.Txn(context.TODO())
	txnResp, err := txn.If(clientv3.Compare(clientv3.CreateRevision("bar"), "=", 0)).
		Then(clientv3.OpPut("bar", "foo", clientv3.WithLease(resp.ID))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		t.Fatalf("failed to init '%s', internal error: %v", key, err)
	}
	if !txnResp.Succeeded {
		if len(txnResp.Responses[0].GetResponseRange().Kvs) > 0 {
			t.Fatalf("key '%s' already exist, and version is %d", key, txnResp.Responses[0].GetResponseRange().Kvs[0].Version)
		}
		t.Fatalf("'%s' init failed, resp: %v", key, txnResp)
	}

	_, kaerr := client.KeepAlive(context.TODO(), resp.ID)
	if kaerr != nil {
		t.Fatal(kaerr)
	}

	time.Sleep(time.Second * 20)
}
