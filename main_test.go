package main

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

type mock struct {
	count atomic.Uint32
}

func (m *mock) Respond(context.Context, request) (response, error) {
	m.count.Add(1)
	return response{}, nil
}

func Test_Proxy_Respond(t *testing.T) {
	tests := []struct {
		name        string
		opts        []Option
		numRequests int
	}{
		{
			name:        "zero buffer, sequential",
			opts:        nil,
			numRequests: 1,
		},
		{
			name:        "buffer size 1, sequential",
			opts:        []Option{WithBuffSize(1)},
			numRequests: 1,
		},
		{
			name:        "buffer size 10, sequential",
			opts:        []Option{WithBuffSize(10)},
			numRequests: 5,
		},
		{
			name:        "zero buffer, concurrent",
			opts:        []Option{WithConcurrency()},
			numRequests: 1,
		},
		{
			name:        "buffer size 1, concurrent",
			opts:        []Option{WithBuffSize(1), WithConcurrency()},
			numRequests: 1,
		},
		{
			name:        "buffer size 10, concurrent",
			opts:        []Option{WithBuffSize(10), WithConcurrency()},
			numRequests: 5,
		},
		{
			name:        "zero buffer, with semaphore",
			opts:        []Option{WithSemaphore(semaphore.NewWeighted(5), 1)},
			numRequests: 1,
		},
		{
			name:        "buffer size 10, with semaphore weight 1",
			opts:        []Option{WithBuffSize(10), WithSemaphore(semaphore.NewWeighted(5), 1)},
			numRequests: 5,
		},
		{
			name:        "buffer size 10, with semaphore weight 2",
			opts:        []Option{WithBuffSize(10), WithSemaphore(semaphore.NewWeighted(10), 2)},
			numRequests: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Given
			m := mock{}
			ctx := context.Background()

			// When
			p, newErr := newProxy(&m, tt.opts...)
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
	underlying := responderFunc(func(ctx context.Context, req request) (response, error) {
		return response{}, nil
	})
	s := &safe{underlying: underlying}

	// When
	resp, err := s.Respond(context.Background(), request{})

	// Then
	assert.NoError(t, err)
	assert.Equal(t, response{}, resp)
}

func Test_Safe_Respond_Error(t *testing.T) {
	t.Parallel()

	// Given
	expectedErr := assert.AnError
	underlying := responderFunc(func(ctx context.Context, req request) (response, error) {
		return response{}, expectedErr
	})
	s := &safe{underlying: underlying}

	// When
	_, err := s.Respond(context.Background(), request{})

	// Then
	assert.ErrorIs(t, err, expectedErr)
}

func Test_Safe_Respond_Panic(t *testing.T) {
	t.Parallel()

	// Given
	underlying := responderFunc(func(ctx context.Context, req request) (response, error) {
		panic("something went wrong")
	})
	s := &safe{underlying: underlying}

	// When
	resp, err := s.Respond(context.Background(), request{})

	// Then
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panicked")
	assert.Equal(t, response{}, resp)
}
