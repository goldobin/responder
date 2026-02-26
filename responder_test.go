package responder_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/goldobin/responder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func Test_Func(t *testing.T) {
	t.Parallel()

	// Given
	targetFn := func(_ context.Context, req int) (string, error) {
		return fmt.Sprintf("got:%d", req), nil
	}
	r := responder.Func(targetFn)

	// When
	resp, err := r.Respond(context.Background(), 42)

	// Then
	require.NoError(t, err)
	assert.Equal(t, "got:42", resp)
}

func Test_Same(t *testing.T) {
	t.Parallel()

	// Given
	r := responder.Same[request]("always")

	// When
	resp, err := r.Respond(context.Background(), request{})

	// Then
	require.NoError(t, err)
	assert.Equal(t, "always", resp)
}

func Test_Error(t *testing.T) {
	t.Parallel()

	// Given
	r := responder.Error[request, response](assert.AnError)

	// When
	resp, err := r.Respond(context.Background(), request{})

	// Then
	assert.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, response{}, resp)
}

func Test_Safe(t *testing.T) {
	// Given
	var (
		target = responder.Same[request](response{})
		s      = responder.Safe(target)
	)

	// When
	resp, err := s.Respond(context.Background(), request{})

	// Then
	require.NoError(t, err)
	assert.Equal(t, response{}, resp)
}

func Test_SafeError(t *testing.T) {
	t.Parallel()

	// Given
	var (
		target = responder.Error[request, response](assert.AnError)
		s      = responder.Safe(target)
	)

	// When
	_, err := s.Respond(context.Background(), request{})

	// Then
	assert.ErrorIs(t, err, assert.AnError)
}

func Test_SafePanic(t *testing.T) {
	t.Parallel()

	// Given
	var (
		targetFn = func(context.Context, request) (response, error) {
			panic("something went wrong")
		}
		s = responder.Safe(responder.Func(targetFn))
	)

	// When
	resp, err := s.Respond(context.Background(), request{})

	// Then
	assert.ErrorIs(t, err, responder.Panicked)
	assert.Equal(t, response{}, resp)
}

func Test_FanOut(t *testing.T) {
	t.Parallel()

	// Given
	var count atomic.Uint32
	rs := []responder.Responder[request, int]{
		responder.Func(func(context.Context, request) (int, error) {
			count.Add(1)
			return 1, nil
		}),
		responder.Func(func(context.Context, request) (int, error) {
			count.Add(1)
			return 2, nil
		}),
		responder.Func(func(context.Context, request) (int, error) {
			count.Add(1)
			return 3, nil
		}),
	}
	fanOut := responder.FanOut(rs)

	// When
	responses, err := fanOut.Respond(context.Background(), request{})

	// Then
	require.NoError(t, err)
	assert.Len(t, responses, 3)
	assert.Equal(t, []int{1, 2, 3}, responses)
	assert.Equal(t, uint32(3), count.Load())
}

func Test_FanOutError(t *testing.T) {
	t.Parallel()

	// Given - mix of successful and failing responders
	var (
		err1 = errors.New("error 1")
		err2 = errors.New("error 2")
		rs   = []responder.Responder[request, int]{
			responder.Func(func(context.Context, request) (int, error) {
				return 1, nil
			}),
			responder.Func(func(context.Context, request) (int, error) {
				return 0, err1
			}),
			responder.Func(func(context.Context, request) (int, error) {
				return 3, nil
			}),
			responder.Func(func(context.Context, request) (int, error) {
				return 0, err2
			}),
		}
		fanOut = responder.FanOut(rs)
	)

	// When
	responses, err := fanOut.Respond(context.Background(), request{})

	// Then - all responders called, errors joined
	assert.ErrorIs(t, err, err1)
	assert.ErrorIs(t, err, err2)
	assert.Len(t, responses, 4)
	assert.Equal(t, []int{1, 0, 3, 0}, responses)
}

func Test_FanOutEmpty(t *testing.T) {
	t.Parallel()

	// Given
	fanOut := responder.FanOut[request, int](nil)

	// When
	responses, err := fanOut.Respond(context.Background(), request{})

	// Then
	require.NoError(t, err)
	assert.Empty(t, responses)
}

func Test_FanOutSingle(t *testing.T) {
	t.Parallel()

	// Given
	rs := []responder.Responder[request, int]{
		responder.Func(func(context.Context, request) (int, error) {
			return 42, nil
		}),
	}
	fanOut := responder.FanOut(rs)

	// When
	responses, err := fanOut.Respond(context.Background(), request{})

	// Then
	require.NoError(t, err)
	assert.Equal(t, []int{42}, responses)
}
