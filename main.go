package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

type request struct{}

type response struct{}

type responder interface {
	Respond(context.Context, request) (response, error)
}

type responderFunc func(context.Context, request) (response, error)

func (f responderFunc) Respond(ctx context.Context, req request) (response, error) {
	return f(ctx, req)
}

type responseEnvelope struct {
	r   response
	err error
}
type requestEnvelope struct {
	ctx    context.Context
	r      request
	respCh chan<- responseEnvelope
}

type proxy struct {
	underlying responder
	requests   chan<- requestEnvelope

	closed   atomic.Bool
	finished <-chan struct{}
}

func (p *proxy) Respond(ctx context.Context, req request) (response, error) {
	if p.closed.Load() {
		return response{}, errors.New("closed")
	}

	respCh := make(chan responseEnvelope, 1)

	select {
	case <-ctx.Done():
		return response{}, fmt.Errorf("context done: %w", ctx.Err())
	case p.requests <- requestEnvelope{
		ctx:    ctx,
		r:      req,
		respCh: respCh,
	}:
	}

	select {
	case <-ctx.Done():
		return response{}, fmt.Errorf("context done: %w", ctx.Err())
	case resp := <-respCh:
		return resp.r, resp.err
	}
}

func (p *proxy) Close(ctx context.Context) error {
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

type options struct {
	buffSize    int
	concurrency bool
	sem         *semaphore.Weighted
	weight      int64
}

type Option func(options *options) error

func WithBuffSize(buffSize int) Option {
	return func(options *options) error {
		if buffSize <= 0 {
			return errors.New("buffSize must be greater than zero")
		}
		options.buffSize = buffSize
		return nil
	}
}

func WithConcurrency() Option {
	return func(options *options) error {
		options.concurrency = true
		return nil
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

func newProxy(r responder, opts ...Option) (*proxy, error) {
	var cfg options
	for _, opt := range opts {
		err := opt(&cfg)
		if err != nil {
			return nil, err
		}
	}

	requests := make(chan requestEnvelope, cfg.buffSize)
	finished := make(chan struct{})
	respond := func(req requestEnvelope) {
		var resp responseEnvelope
		resp.r, resp.err = r.Respond(req.ctx, req.r)
		req.respCh <- resp
	}

	go func() {
		defer close(finished)
		for req := range requests {
			if cfg.sem == nil {
				respond(req)
				continue
			}

			if err := cfg.sem.Acquire(req.ctx, cfg.weight); err != nil {
				req.respCh <- responseEnvelope{err: fmt.Errorf("failed to acquire semaphore: %w", err)}
				continue
			}

			go func() {
				defer cfg.sem.Release(cfg.weight)
				respond(req)
			}()
		}
	}()

	return &proxy{
		requests:   requests,
		underlying: r,
		finished:   finished,
	}, nil
}

type safe struct {
	underlying responder
}

func (r *safe) Respond(ctx context.Context, req request) (resp response, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("underlying responder panicked")
		}
	}()

	resp, err = r.underlying.Respond(ctx, req)
	return
}
