package exec

import (
    "errors"
    me "github.com/rwn3120/go-multierror"
)

type Success struct{}

func (s *Success) Err() error {
    return nil
}

type Failed struct {
    multiError *me.MultiError
}

func (f *Failed) Err() error {
    return f.multiError.ErrorOrNil()
}

func (f *Failed) Errors() []error {
    return f.multiError.ErrorsOrNil()
}

type Expired struct{}

func (e *Expired) Err() error {
    return errors.New("job expired")
}

func NewResult(errors ...error) Result {
    if len(errors) > 0 {
        return &Failed{me.New(errors...)}
    } else {
        return &Success{}
    }
}