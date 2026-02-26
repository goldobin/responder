package responder_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/goldobin/responder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	kvGetRequest  struct{ key string }
	kvGetResponse struct{ value string }
)

type (
	kvPutRequest  struct{ key, value string }
	kvPutResponse struct{}
)

type kvService interface {
	get(ctx context.Context, req kvGetRequest) (kvGetResponse, error)
	put(ctx context.Context, req kvPutRequest) (kvPutResponse, error)
}

type inMemoryKV struct {
	dataMu sync.RWMutex
	data   map[string]string
}

func newInMemoryKV() *inMemoryKV {
	return &inMemoryKV{data: make(map[string]string)}
}

func (kv *inMemoryKV) get(_ context.Context, req kvGetRequest) (kvGetResponse, error) {
	kv.dataMu.RLock()
	defer kv.dataMu.RUnlock()
	return kvGetResponse{value: kv.data[req.key]}, nil
}

func (kv *inMemoryKV) put(_ context.Context, req kvPutRequest) (kvPutResponse, error) {
	kv.dataMu.Lock()
	defer kv.dataMu.Unlock()
	kv.data[req.key] = req.value
	return kvPutResponse{}, nil
}

type kvServiceProxy struct {
	getProxy *responder.Proxy[kvGetRequest, kvGetResponse]
	putProxy *responder.Proxy[kvPutRequest, kvPutResponse]
}

func newKVServiceProxy(svc kvService) *kvServiceProxy {
	opts := []responder.Option{
		responder.WithBuffer(10),
		responder.WithBoundConcurrency(2),
	}

	getProxy := responder.NewProxy(responder.Func(svc.get), opts...)
	putProxy := responder.NewProxy(responder.Func(svc.put), opts...)

	return &kvServiceProxy{
		getProxy: getProxy,
		putProxy: putProxy,
	}
}

func (p *kvServiceProxy) get(ctx context.Context, req kvGetRequest) (kvGetResponse, error) {
	return p.getProxy.Respond(ctx, req)
}

func (p *kvServiceProxy) put(ctx context.Context, req kvPutRequest) (kvPutResponse, error) {
	return p.putProxy.Respond(ctx, req)
}

func (p *kvServiceProxy) close() error {
	getErr := p.getProxy.Close()
	putErr := p.putProxy.Close()
	if getErr != nil {
		return getErr
	}
	return putErr
}

func Test_kvServiceProxy(t *testing.T) {
	t.Parallel()

	// Given
	ctx := context.Background()
	kv := newInMemoryKV()
	proxy := newKVServiceProxy(kv)
	defer func() {
		_ = proxy.close()
	}()

	// When - put values
	_, err := proxy.put(ctx, kvPutRequest{key: "foo", value: "bar"})
	assert.NoError(t, err)

	_, err = proxy.put(ctx, kvPutRequest{key: "hello", value: "world"})
	assert.NoError(t, err)

	// When - get values
	resp1, err := proxy.get(ctx, kvGetRequest{key: "foo"})
	assert.NoError(t, err)
	assert.Equal(t, "bar", resp1.value)

	resp2, err := proxy.get(ctx, kvGetRequest{key: "hello"})
	assert.NoError(t, err)
	assert.Equal(t, "world", resp2.value)

	// When - get non-existent key
	resp3, err := proxy.get(ctx, kvGetRequest{key: "missing"})
	assert.NoError(t, err)
	assert.Equal(t, "", resp3.value)
}

func Test_kvServiceProxy_New(t *testing.T) {
	t.Parallel()

	// Given
	kv := newInMemoryKV()

	// When
	proxy := newKVServiceProxy(kv)

	// Then
	require.NotNil(t, proxy)
	_ = proxy.close()
}

func Example_kvServiceProxy() {
	ctx := context.Background()

	// Create target service
	kv := newInMemoryKV()

	// Create buffered service proxy
	proxy := newKVServiceProxy(kv)
	defer func() {
		_ = proxy.close()
	}()

	// Put some values
	_, _ = proxy.put(ctx, kvPutRequest{key: "greeting", value: "hello"})
	_, _ = proxy.put(ctx, kvPutRequest{key: "name", value: "world"})

	// Get values back
	resp1, _ := proxy.get(ctx, kvGetRequest{key: "greeting"})
	resp2, _ := proxy.get(ctx, kvGetRequest{key: "name"})

	fmt.Printf("%s, %s!\n", resp1.value, resp2.value)

	// Output: hello, world!
}
