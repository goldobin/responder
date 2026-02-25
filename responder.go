package responder

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/semaphore"
)

type (
	Responder[T any, R any] interface {
		Respond(context.Context, T) (R, error)
	}
	Proxy[T any, R any] struct {
		requestsMu sync.RWMutex
		requests   chan<- requestEnvelope[T, R]
		target     Responder[T, R]
		drained    <-chan struct{}
	}
	Option  func(opts *options)
	options struct {
		buffSize    int
		concurrency bool
		sem         *semaphore.Weighted
		weight      int64
	}
	responseEnvelope[R any] struct {
		resp R
		err  error
	}
	requestEnvelope[T any, R any] struct {
		ctx    context.Context
		req    T
		respCh chan<- responseEnvelope[R]
	}
	responderFn[T any, R any] func(context.Context, T) (R, error)
)

var Closed = errors.New("closed")
var Panicked = errors.New("panicked")

func (f responderFn[T, R]) Respond(ctx context.Context, req T) (R, error) {
	return f(ctx, req)
}

func Func[T any, R any](fn func(context.Context, T) (R, error)) Responder[T, R] {
	return responderFn[T, R](fn)
}

func NewProxy[T any, R any](target Responder[T, R], opts ...Option) *Proxy[T, R] {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}

	requests := make(chan requestEnvelope[T, R], cfg.buffSize)
	drained := make(chan struct{})
	respond := func(req requestEnvelope[T, R]) {
		var resp responseEnvelope[R]
		resp.resp, resp.err = target.Respond(req.ctx, req.req)
		req.respCh <- resp
	}

	go func() {
		defer close(drained)
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
		requests: requests,
		target:   target,
		drained:  drained,
	}
}

func (p *Proxy[T, R]) Respond(ctx context.Context, req T) (R, error) {
	respCh, err := p.send(ctx, req)

	var zero R
	if err != nil {
		return zero, err
	}

	select {
	case <-ctx.Done():
		var zero R
		return zero, fmt.Errorf("receive response: %w", ctx.Err())
	case resp := <-respCh:
		return resp.resp, resp.err
	}
}

func (p *Proxy[T, R]) Close() error {
	p.requestsMu.Lock()
	defer p.requestsMu.Unlock()
	if p.requests == nil {
		return Closed
	}
	close(p.requests)
	p.requests = nil
	return nil
}

func (p *Proxy[T, R]) Drained() <-chan struct{} {
	return p.drained
}

func (p *Proxy[T, R]) send(ctx context.Context, req T) (<-chan responseEnvelope[R], error) {
	p.requestsMu.RLock()
	defer p.requestsMu.RUnlock()
	if p.requests == nil {
		return nil, Closed
	}

	var (
		respCh = make(chan responseEnvelope[R], 1)
		reqEnv = requestEnvelope[T, R]{ctx: ctx, req: req, respCh: respCh}
	)
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("send request: %w", ctx.Err())
	case p.requests <- reqEnv:
	}

	return respCh, nil
}

func WithBuffer(buffSize int) Option {
	return func(opts *options) {
		if buffSize <= 0 {
			panic("buffSize must be greater than zero")
		}
		opts.buffSize = buffSize
	}
}

func WithUnboundConcurrency() Option {
	return func(opts *options) {
		opts.concurrency = true
	}
}

func WithBoundConcurrency(maxWorkers int) Option {
	return func(opts *options) {
		if maxWorkers < 1 {
			panic("maxWorkers must be at least 1")
		}

		sem := semaphore.NewWeighted(int64(maxWorkers))
		WithSemaphore(sem, 1)(opts)
	}
}

func WithSemaphore(sem *semaphore.Weighted, requestWeight int64) Option {
	return func(options *options) {
		var errs []error
		if sem == nil {
			errs = append(errs, errors.New("semaphore cannot be nil"))
		}
		if requestWeight < 1 {
			errs = append(errs, errors.New("weight must be at least 1"))
		}

		if err := errors.Join(errs...); err != nil {
			panic(err.Error())
		}

		options.concurrency = true
		options.sem = sem
		options.weight = requestWeight
	}
}

func Safe[T any, R any](r Responder[T, R]) Responder[T, R] {
	return Func(func(ctx context.Context, req T) (resp R, err error) {
		defer func() {
			if recover() != nil {
				err = Panicked
			}
		}()

		resp, err = r.Respond(ctx, req)
		return
	})
}

func Same[T any, R any](r R) Responder[T, R] {
	return Func(func(context.Context, T) (R, error) {
		return r, nil
	})
}

func Error[T any, R any](err error) Responder[T, R] {
	var zero R
	return Func(func(context.Context, T) (R, error) {
		return zero, err
	})
}

func FanOut[T any, R any](rs []Responder[T, R]) Responder[T, []R] {
	return Func(func(ctx context.Context, req T) ([]R, error) {
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
