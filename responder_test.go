package responder_test

import (
	"context"
	"sync/atomic"
	"testing"

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

func Test_Proxy_Respond(t *testing.T) {
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

func Test_Safe_Respond_Success(t *testing.T) {
	// Given
	underlying := responder.Same[request, response](response{})
	s := responder.Safe(underlying)

	// When
	resp, err := s.Respond(context.Background(), request{})

	// Then
	assert.NoError(t, err)
	assert.Equal(t, response{}, resp)
}

func Test_Safe_Respond_Error(t *testing.T) {
	t.Parallel()

	// Given
	underlying := responder.Error[request, response](assert.AnError)
	s := responder.Safe(underlying)

	// When
	_, err := s.Respond(context.Background(), request{})

	// Then
	assert.ErrorIs(t, err, assert.AnError)
}

func Test_Safe_Respond_Panic(t *testing.T) {
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
