package exec

import (
    "errors"
    me "github.com/rwn3120/go-multierror"
)

type Result interface {
    Err() error
}

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

func Ok() Result {
    return &Success{}
}

func Nok(err error, other ...error) Result {
    allErrors := append([]error{err}, other...)
    return &Failed{me.New(allErrors...)}
}

func NewResult(errors ...error) Result {
    filteredErrors := make([]error, len(errors))
    for _, err := range errors {
        if err != nil {
            filteredErrors = append(filteredErrors, err)
        }
    }
    if len(filteredErrors) > 0 {
        return Nok(filteredErrors[0], filteredErrors[1:]...)
    } else {
        return Ok()
    }
}
