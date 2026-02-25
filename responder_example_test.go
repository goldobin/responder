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

// Request/Response types for Get operation
type (
	KVGetRequest  struct{ Key string }
	KVGetResponse struct{ Value string }
)

// Request/Response types for Put operation
type (
	KVPutRequest  struct{ Key, Value string }
	KVPutResponse struct{}
)

// KVService is the target service interface
type KVService interface {
	Get(ctx context.Context, req KVGetRequest) (KVGetResponse, error)
	Put(ctx context.Context, req KVPutRequest) (KVPutResponse, error)
}

// InMemoryKV is a simple in-memory implementation
type InMemoryKV struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewInMemoryKV() *InMemoryKV {
	return &InMemoryKV{data: make(map[string]string)}
}

func (kv *InMemoryKV) Get(_ context.Context, req KVGetRequest) (KVGetResponse, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return KVGetResponse{Value: kv.data[req.Key]}, nil
}

func (kv *InMemoryKV) Put(_ context.Context, req KVPutRequest) (KVPutResponse, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[req.Key] = req.Value
	return KVPutResponse{}, nil
}

// KVServiceProxy is a composite proxy that buffers both Get and Put operations
type KVServiceProxy struct {
	getProxy *responder.Proxy[KVGetRequest, KVGetResponse]
	putProxy *responder.Proxy[KVPutRequest, KVPutResponse]
}

func NewKVServiceProxy(svc KVService) *KVServiceProxy {
	opts := []responder.Option{
		responder.WithBuffer(10),
		responder.WithBoundConcurrency(2),
	}

	getResponder := responder.Func(svc.Get)
	putResponder := responder.Func(svc.Put)

	getProxy := responder.NewProxy(getResponder, opts...)
	putProxy := responder.NewProxy(putResponder, opts...)

	return &KVServiceProxy{
		getProxy: getProxy,
		putProxy: putProxy,
	}
}

func (p *KVServiceProxy) Get(ctx context.Context, req KVGetRequest) (KVGetResponse, error) {
	return p.getProxy.Respond(ctx, req)
}

func (p *KVServiceProxy) Put(ctx context.Context, req KVPutRequest) (KVPutResponse, error) {
	return p.putProxy.Respond(ctx, req)
}

func (p *KVServiceProxy) Close() error {
	getErr := p.getProxy.Close()
	putErr := p.putProxy.Close()
	if getErr != nil {
		return getErr
	}
	return putErr
}

func Test_KVServiceProxy(t *testing.T) {
	t.Parallel()

	// Given
	ctx := context.Background()
	kv := NewInMemoryKV()
	proxy := NewKVServiceProxy(kv)
	defer func() {
		_ = proxy.Close()
	}()

	// When - Put values
	_, err := proxy.Put(ctx, KVPutRequest{Key: "foo", Value: "bar"})
	assert.NoError(t, err)

	_, err = proxy.Put(ctx, KVPutRequest{Key: "hello", Value: "world"})
	assert.NoError(t, err)

	// When - Get values
	resp1, err := proxy.Get(ctx, KVGetRequest{Key: "foo"})
	assert.NoError(t, err)
	assert.Equal(t, "bar", resp1.Value)

	resp2, err := proxy.Get(ctx, KVGetRequest{Key: "hello"})
	assert.NoError(t, err)
	assert.Equal(t, "world", resp2.Value)

	// When - Get non-existent key
	resp3, err := proxy.Get(ctx, KVGetRequest{Key: "missing"})
	assert.NoError(t, err)
	assert.Equal(t, "", resp3.Value)
}

func Test_KVServiceProxy_New(t *testing.T) {
	t.Parallel()

	// Given
	kv := NewInMemoryKV()

	// When
	proxy := NewKVServiceProxy(kv)

	// Then
	require.NotNil(t, proxy)
	_ = proxy.Close()
}

func Example_kvServiceProxy() {
	ctx := context.Background()

	// Create target service
	kv := NewInMemoryKV()

	// Create buffered service proxy
	proxy := NewKVServiceProxy(kv)
	defer func() {
		_ = proxy.Close()
	}()

	// Put some values
	_, _ = proxy.Put(ctx, KVPutRequest{Key: "greeting", Value: "hello"})
	_, _ = proxy.Put(ctx, KVPutRequest{Key: "name", Value: "world"})

	// Get values back
	resp1, _ := proxy.Get(ctx, KVGetRequest{Key: "greeting"})
	resp2, _ := proxy.Get(ctx, KVGetRequest{Key: "name"})

	fmt.Printf("%s, %s!\n", resp1.Value, resp2.Value)

	// Output: hello, world!
}
