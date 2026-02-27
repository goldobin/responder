package responder

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/semaphore"
)

type (
	Proxy[T any, R any] struct {
		requestsMu sync.RWMutex
		requests   chan<- requestEnvelope[T, R]
		targetMu   sync.RWMutex
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
)

func NewProxy[T any, R any](opts ...Option) *Proxy[T, R] {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}

	var (
		p        Proxy[T, R]
		requests = make(chan requestEnvelope[T, R], cfg.buffSize)
		drained  = make(chan struct{})
		respond  = func(req requestEnvelope[T, R]) {
			var (
				resp responseEnvelope[R]
				t    = p.Target()
			)

			if t == nil {
				resp.err = NoTarget
			} else {
				resp.resp, resp.err = t.Respond(req.ctx, req.req)
			}
			req.respCh <- resp
		}
	)

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

	p.requests = requests
	p.drained = drained
	return &p
}

func NewProxyWithTarget[T any, R any](target Responder[T, R], opts ...Option) *Proxy[T, R] {
	p := NewProxy[T, R](opts...)
	p.SetTarget(target)
	return p
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

func (p *Proxy[T, R]) SetTarget(target Responder[T, R]) {
	p.targetMu.Lock()
	defer p.targetMu.Unlock()
	p.target = target
}

func (p *Proxy[T, R]) Target() Responder[T, R] {
	p.targetMu.RLock()
	defer p.targetMu.RUnlock()
	return p.target
}

func (p *Proxy[T, R]) Close() {
	p.requestsMu.Lock()
	defer p.requestsMu.Unlock()
	if p.requests == nil {
		return
	}
	close(p.requests)
	p.requests = nil
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
