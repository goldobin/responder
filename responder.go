package responder

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

type (
	Responder[T any, R any] interface {
		Respond(context.Context, T) (R, error)
	}
	Func[T any, R any]  func(context.Context, T) (R, error)
	Proxy[T any, R any] struct {
		underlying Responder[T, R]
		requests   chan<- requestEnvelope[T, R]

		closed   atomic.Bool
		finished <-chan struct{}
	}
	options struct {
		buffSize    int
		concurrency bool
		sem         *semaphore.Weighted
		weight      int64
	}
	responseEnvelope[R any] struct {
		r   R
		err error
	}
	requestEnvelope[T any, R any] struct {
		ctx    context.Context
		t      T
		respCh chan<- responseEnvelope[R]
	}
)

func (f Func[T, R]) Respond(ctx context.Context, req T) (R, error) {
	return f(ctx, req)
}

func (p *Proxy[T, R]) Respond(ctx context.Context, req T) (R, error) {
	var zero R
	if p.closed.Load() {
		return zero, errors.New("closed")
	}

	respCh := make(chan responseEnvelope[R], 1)
	select {
	case <-ctx.Done():
		return zero, fmt.Errorf("context done: %w", ctx.Err())
	case p.requests <- requestEnvelope[T, R]{
		ctx:    ctx,
		t:      req,
		respCh: respCh,
	}:
	}

	select {
	case <-ctx.Done():
		return zero, fmt.Errorf("context done: %w", ctx.Err())
	case resp := <-respCh:
		return resp.r, resp.err
	}
}

func (p *Proxy[T, R]) Close(ctx context.Context) error {
	if !p.closed.CompareAndSwap(false, true) {
		return errors.New("already closed")
	}

	close(p.requests)

	select {
	case <-ctx.Done():
		return fmt.Errorf("context done: %w", ctx.Err())
	case <-p.finished:
		return nil
	}
}

type Option func(opts *options) error

func WithBuffer(buffSize int) Option {
	return func(opts *options) error {
		if buffSize <= 0 {
			return errors.New("buffSize must be greater than zero")
		}
		opts.buffSize = buffSize
		return nil
	}
}

func WithUnboundConcurrency() Option {
	return func(opts *options) error {
		opts.concurrency = true
		return nil
	}
}

func WithBoundConcurrency(maxWorkers int) Option {
	return func(opts *options) error {
		if maxWorkers < 1 {
			return errors.New("maxWorkers must be at least 1")
		}

		sem := semaphore.NewWeighted(int64(maxWorkers))
		return WithSemaphore(sem, 1)(opts)
	}
}

func WithSemaphore(sem *semaphore.Weighted, requestWeight int64) Option {
	return func(options *options) error {
		var errs []error
		if sem == nil {
			errs = append(errs, errors.New("semaphore cannot be nil"))
		}
		if requestWeight < 1 {
			errs = append(errs, errors.New("weight must be at least 1"))
		}

		if err := errors.Join(errs...); err != nil {
			return err
		}

		options.concurrency = true
		options.sem = sem
		options.weight = requestWeight
		return nil
	}
}

func NewProxy[T any, R any](r Responder[T, R], opts ...Option) (*Proxy[T, R], error) {
	var cfg options
	for _, opt := range opts {
		err := opt(&cfg)
		if err != nil {
			return nil, err
		}
	}

	requests := make(chan requestEnvelope[T, R], cfg.buffSize)
	finished := make(chan struct{})
	respond := func(req requestEnvelope[T, R]) {
		var resp responseEnvelope[R]
		resp.r, resp.err = r.Respond(req.ctx, req.t)
		req.respCh <- resp
	}

	go func() {
		defer close(finished)
		for req := range requests {
			if cfg.sem == nil {
				if cfg.concurrency {
					// Unbound concurrency case
					go respond(req)
					continue
				} else {
					// Sequential execution
					respond(req)
				}
				continue
			}

			// Bound concurrency managed by semaphore
			if err := cfg.sem.Acquire(req.ctx, cfg.weight); err != nil {
				req.respCh <- responseEnvelope[R]{err: fmt.Errorf("failed to acquire semaphore: %w", err)}
				continue
			}

			go func() {
				defer cfg.sem.Release(cfg.weight)
				respond(req)
			}()
		}
	}()

	return &Proxy[T, R]{
		requests:   requests,
		underlying: r,
		finished:   finished,
	}, nil
}

func Safe[T any, R any](r Responder[T, R]) Responder[T, R] {
	return Func[T, R](func(ctx context.Context, req T) (resp R, err error) {
		defer func() {
			if recover() != nil {
				err = errors.New("underlying responder panicked")
			}
		}()

		resp, err = r.Respond(ctx, req)
		return
	})
}

func Same[T any, R any](r R) Responder[T, R] {
	return Func[T, R](func(context.Context, T) (R, error) {
		return r, nil
	})
}

func Error[T any, R any](err error) Responder[T, R] {
	var zero R
	return Func[T, R](func(context.Context, T) (R, error) {
		return zero, err
	})
}

func FanOut[T any, R any](rs []Responder[T, R]) Responder[T, []R] {
	return Func[T, []R](func(ctx context.Context, req T) ([]R, error) {
		responses := make([]R, len(rs))
		var errs []error
		for i, r := range rs {
			resp, err := r.Respond(ctx, req)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			responses[i] = resp
		}

		return responses, errors.Join(errs...)
	})
}
