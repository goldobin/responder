package responder_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goldobin/responder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

type (
	mock     struct{ count atomic.Uint32 }
	request  struct{}
	response struct{}
)

func (m *mock) Respond(context.Context, request) (response, error) {
	m.count.Add(1)
	return response{}, nil
}

func Test_Safe(t *testing.T) {
	// Given
	underlying := responder.Same[request, response](response{})
	s := responder.Safe(underlying)

	// When
	resp, err := s.Respond(context.Background(), request{})

	// Then
	assert.NoError(t, err)
	assert.Equal(t, response{}, resp)
}

func Test_SafeError(t *testing.T) {
	t.Parallel()

	// Given
	underlying := responder.Error[request, response](assert.AnError)
	s := responder.Safe(underlying)

	// When
	_, err := s.Respond(context.Background(), request{})

	// Then
	assert.ErrorIs(t, err, assert.AnError)
}

func Test_SafePanic(t *testing.T) {
	t.Parallel()

	// Given
	underlying := responder.Func[request, response](func(context.Context, request) (response, error) {
		panic("something went wrong")
	})
	s := responder.Safe(underlying)

	// When
	resp, err := s.Respond(context.Background(), request{})

	// Then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panicked")
	assert.Equal(t, response{}, resp)
}

func Test_FanOut(t *testing.T) {
	t.Parallel()

	// Given
	var count atomic.Uint32
	rs := []responder.Responder[request, int]{
		responder.Func[request, int](func(context.Context, request) (int, error) {
			count.Add(1)
			return 1, nil
		}),
		responder.Func[request, int](func(context.Context, request) (int, error) {
			count.Add(1)
			return 2, nil
		}),
		responder.Func[request, int](func(context.Context, request) (int, error) {
			count.Add(1)
			return 3, nil
		}),
	}
	fanOut := responder.FanOut(rs)

	// When
	responses, err := fanOut.Respond(context.Background(), request{})

	// Then
	assert.NoError(t, err)
	assert.Len(t, responses, 3)
	assert.Equal(t, []int{1, 2, 3}, responses)
	assert.Equal(t, uint32(3), count.Load())
}

func Test_FanOutError(t *testing.T) {
	t.Parallel()

	// Given - mix of successful and failing responders
	err1 := errors.New("error 1")
	err2 := errors.New("error 2")
	rs := []responder.Responder[request, int]{
		responder.Func[request, int](func(context.Context, request) (int, error) {
			return 1, nil
		}),
		responder.Func[request, int](func(context.Context, request) (int, error) {
			return 0, err1
		}),
		responder.Func[request, int](func(context.Context, request) (int, error) {
			return 3, nil
		}),
		responder.Func[request, int](func(context.Context, request) (int, error) {
			return 0, err2
		}),
	}
	fanOut := responder.FanOut(rs)

	// When
	responses, err := fanOut.Respond(context.Background(), request{})

	// Then - all responders called, errors joined
	assert.Error(t, err)
	assert.ErrorIs(t, err, err1)
	assert.ErrorIs(t, err, err2)
	assert.Len(t, responses, 4)
	assert.Equal(t, []int{1, 0, 3, 0}, responses)
}

func Test_Proxy(t *testing.T) {
	tests := []struct {
		name        string
		opts        []responder.Option
		numRequests int
	}{
		{
			name:        "zero buffer, sequential",
			opts:        nil,
			numRequests: 1,
		},
		{
			name:        "buffer size 1, sequential",
			opts:        []responder.Option{responder.WithBuffer(1)},
			numRequests: 1,
		},
		{
			name:        "buffer size 10, sequential",
			opts:        []responder.Option{responder.WithBuffer(10)},
			numRequests: 5,
		},
		{
			name:        "zero buffer, concurrent",
			opts:        []responder.Option{responder.WithUnboundConcurrency()},
			numRequests: 1,
		},
		{
			name:        "buffer size 1, concurrent",
			opts:        []responder.Option{responder.WithBuffer(1), responder.WithUnboundConcurrency()},
			numRequests: 1,
		},
		{
			name:        "buffer size 10, concurrent",
			opts:        []responder.Option{responder.WithBuffer(10), responder.WithUnboundConcurrency()},
			numRequests: 5,
		},
		{
			name:        "zero buffer, with semaphore",
			opts:        []responder.Option{responder.WithSemaphore(semaphore.NewWeighted(5), 1)},
			numRequests: 1,
		},
		{
			name:        "buffer size 10, with semaphore weight 1",
			opts:        []responder.Option{responder.WithBuffer(10), responder.WithSemaphore(semaphore.NewWeighted(5), 1)},
			numRequests: 5,
		},
		{
			name:        "buffer size 10, with semaphore weight 2",
			opts:        []responder.Option{responder.WithBuffer(10), responder.WithSemaphore(semaphore.NewWeighted(10), 2)},
			numRequests: 5,
		},
		{
			name:        "zero buffer, bound concurrency 1",
			opts:        []responder.Option{responder.WithBoundConcurrency(1)},
			numRequests: 1,
		},
		{
			name:        "zero buffer, bound concurrency 5",
			opts:        []responder.Option{responder.WithBoundConcurrency(5)},
			numRequests: 5,
		},
		{
			name:        "buffer size 10, bound concurrency 1",
			opts:        []responder.Option{responder.WithBuffer(10), responder.WithBoundConcurrency(1)},
			numRequests: 5,
		},
		{
			name:        "buffer size 10, bound concurrency 5",
			opts:        []responder.Option{responder.WithBuffer(10), responder.WithBoundConcurrency(5)},
			numRequests: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Given
			m := mock{}
			ctx := context.Background()

			// When
			p, newErr := responder.NewProxy(&m, tt.opts...)
			require.NoError(t, newErr)

			for i := 0; i < tt.numRequests; i++ {
				_, respErr := p.Respond(ctx, request{})
				assert.NoError(t, respErr)
			}

			closeErr := p.Close(ctx)

			// Then
			assert.NoError(t, closeErr)
			assert.Equal(t, uint32(tt.numRequests), m.count.Load())
		})
	}
}

func Test_Proxy_InvalidOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []responder.Option
	}{
		{
			name: "buffer size 0",
			opts: []responder.Option{responder.WithBuffer(0)},
		},
		{
			name: "buffer size negative",
			opts: []responder.Option{responder.WithBuffer(-1)},
		},
		{
			name: "nil semaphore",
			opts: []responder.Option{responder.WithSemaphore(nil, 1)},
		},
		{
			name: "semaphore weight 0",
			opts: []responder.Option{responder.WithSemaphore(semaphore.NewWeighted(1), 0)},
		},
		{
			name: "bound concurrency 0",
			opts: []responder.Option{responder.WithBoundConcurrency(0)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := responder.NewProxy(responder.Same[request, response](response{}), tt.opts...)
			assert.Error(t, err)
		})
	}
}

func Test_Proxy_RespondAfterClose(t *testing.T) {
	t.Parallel()

	// Given
	p, err := responder.NewProxy(responder.Same[request, response](response{}))
	require.NoError(t, err)

	// When
	_ = p.Close(context.Background())
	_, err = p.Respond(context.Background(), request{})

	// Then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func Test_Proxy_DoubleClose(t *testing.T) {
	t.Parallel()

	// Given
	p, err := responder.NewProxy(responder.Same[request, response](response{}))
	require.NoError(t, err)

	// When
	err1 := p.Close(context.Background())
	err2 := p.Close(context.Background())

	// Then
	assert.NoError(t, err1)
	assert.Error(t, err2)
	assert.Contains(t, err2.Error(), "already closed")
}

func Test_Proxy_ContextCancelledBeforeSend(t *testing.T) {
	t.Parallel()

	// Given - unbuffered channel, no reader yet
	blocker := make(chan struct{})
	done := make(chan struct{})
	underlying := responder.Func[request, response](func(ctx context.Context, req request) (response, error) {
		<-blocker // block forever
		return response{}, nil
	})
	p, err := responder.NewProxy(underlying) // unbuffered
	require.NoError(t, err)

	// Start one request to block the worker
	go func() {
		defer close(done)
		_, _ = p.Respond(context.Background(), request{})
	}()
	time.Sleep(10 * time.Millisecond) // let it block

	// When - try to send with already canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = p.Respond(ctx, request{})

	// Then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context done")

	// Cleanup - unblock and wait
	close(blocker)
	<-done
	_ = p.Close(context.Background())
}

func Test_Proxy_ContextCancelledWhileWaitingForResponse(t *testing.T) {
	t.Parallel()

	// Given - responder that blocks until signaled
	proceed := make(chan struct{})
	underlying := responder.Func[request, response](func(ctx context.Context, req request) (response, error) {
		<-proceed
		return response{}, nil
	})
	p, err := responder.NewProxy(underlying, responder.WithBuffer(1))
	require.NoError(t, err)

	// When
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = p.Respond(ctx, request{})

	// Then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context done")

	// Cleanup
	close(proceed)
	_ = p.Close(context.Background())
}

func Test_Proxy_CloseDrainsBufferedRequests(t *testing.T) {
	t.Parallel()

	// Given
	var count atomic.Uint32
	underlying := responder.Func[request, response](func(ctx context.Context, req request) (response, error) {
		count.Add(1)
		return response{}, nil
	})
	p, err := responder.NewProxy(underlying, responder.WithBuffer(10))
	require.NoError(t, err)

	// When - send multiple requests
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			_, _ = p.Respond(context.Background(), request{})
		})
	}

	wg.Wait()
	closeErr := p.Close(context.Background())

	// Then - all requests should be processed
	assert.NoError(t, closeErr)
	assert.Equal(t, uint32(5), count.Load())
}

func Test_Proxy_CloseWithInFlightRequests(t *testing.T) {
	t.Parallel()

	// Given
	started := make(chan struct{})
	proceed := make(chan struct{})
	var count atomic.Uint32

	underlying := responder.Func[request, response](func(ctx context.Context, req request) (response, error) {
		count.Add(1)
		close(started)
		<-proceed
		return response{}, nil
	})
	p, err := responder.NewProxy(underlying, responder.WithBuffer(1))
	require.NoError(t, err)

	// When - start a request that blocks
	go func() {
		_, _ = p.Respond(context.Background(), request{})
	}()
	<-started // wait for request to start processing

	// Close in background
	closeDone := make(chan error)
	go func() {
		closeDone <- p.Close(context.Background())
	}()

	// Let the request complete
	close(proceed)

	// Then
	closeErr := <-closeDone
	assert.NoError(t, closeErr)
	assert.Equal(t, uint32(1), count.Load())
}

// Worker exhaustion

func Test_Proxy_WorkerExhaustion(t *testing.T) {
	t.Parallel()

	// Given - 2 workers, 3 requests
	blocker := make(chan struct{})
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	ready := make(chan struct{}, 3)

	underlying := responder.Func[request, response](func(ctx context.Context, req request) (response, error) {
		current := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if current <= old || maxInFlight.CompareAndSwap(old, current) {
				break
			}
		}
		ready <- struct{}{}
		<-blocker
		inFlight.Add(-1)
		return response{}, nil
	})
	p, err := responder.NewProxy(underlying,
		responder.WithBuffer(10),
		responder.WithBoundConcurrency(2),
	)
	require.NoError(t, err)

	// When - send 3 requests concurrently
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			_, _ = p.Respond(context.Background(), request{})
		})
	}

	// Wait for 2 workers to be busy (third will be waiting for semaphore)
	<-ready
	<-ready
	time.Sleep(10 * time.Millisecond) // small wait for stability

	// Then - max 2 should be in flight
	assert.Equal(t, int32(2), maxInFlight.Load())

	// Cleanup
	close(blocker)
	wg.Wait()
	_ = p.Close(context.Background())
}

func Test_Proxy_SemaphoreAcquisitionCancelledByContext(t *testing.T) {
	t.Parallel()

	// Given - 1 worker, block it
	blocker := make(chan struct{})
	done := make(chan struct{})
	underlying := responder.Func[request, response](func(ctx context.Context, req request) (response, error) {
		<-blocker
		return response{}, nil
	})
	p, err := responder.NewProxy(underlying,
		responder.WithBuffer(10),
		responder.WithBoundConcurrency(1),
	)
	require.NoError(t, err)

	// Start first request to occupy the worker
	go func() {
		defer close(done)
		_, _ = p.Respond(context.Background(), request{})
	}()
	time.Sleep(10 * time.Millisecond)

	// When - second request with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = p.Respond(ctx, request{})

	// Then - should fail due to context timeout while waiting for semaphore
	assert.Error(t, err)

	// Cleanup - unblock and wait
	close(blocker)
	<-done
	_ = p.Close(context.Background())
}

func Test_Proxy_CloseContextTimeout(t *testing.T) {
	t.Parallel()

	// Given - responder that blocks until signaled
	blocker := make(chan struct{})
	started := make(chan struct{})
	underlying := responder.Func[request, response](func(ctx context.Context, req request) (response, error) {
		close(started)
		<-blocker
		return response{}, nil
	})
	p, err := responder.NewProxy(underlying, responder.WithBuffer(1))
	require.NoError(t, err)

	// Start a blocking request
	go func() {
		_, _ = p.Respond(context.Background(), request{})
	}()
	<-started // wait for request to start processing

	// When - close with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closeErr := p.Close(ctx)

	// Then
	assert.Error(t, closeErr)
	assert.Contains(t, closeErr.Error(), "context done")

	close(blocker)
}
