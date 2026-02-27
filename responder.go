package responder

import (
	"context"
	"errors"
)

type (
	Responder[T any, R any] interface {
		Respond(context.Context, T) (R, error)
	}
	responderFn[T any, R any] func(context.Context, T) (R, error)
)

func (f responderFn[T, R]) Respond(ctx context.Context, req T) (R, error) {
	return f(ctx, req)
}

func Func[T any, R any](fn func(context.Context, T) (R, error)) Responder[T, R] {
	return responderFn[T, R](fn)
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
