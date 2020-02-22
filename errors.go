package binx

import "errors"

var (
	ErrNotFound    = errors.New("not found")
	ErrIdxNotFound = errors.New("index not found")
)

const (
	errEmptyKey   = "key cannot be empty"
	errNilPointer = "target must be a pointer to a valid variable"
)
