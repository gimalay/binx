package binx

import (
	"encoding"

	bolt "github.com/coreos/bbolt"
)

type (
	Bucket interface {
		BucketKey() []byte
	}
	Index interface {
		Bucket
		Key() []byte
	}
	UniqueIndex interface {
		Bucket
		UniqueKey() []byte
	}
	Queryable interface {
		Bucket
		AppendBinary(data []byte) (bool, error)
	}
	Indexable interface {
		UniqueIndex
		encoding.BinaryMarshaler
		MasterIndexBucketKey() []byte
		Indexes() []Index
	}

	Reader interface {
		Get(Queryable, []byte) error
		Scan(Queryable, []Bound) error
	}

	Writer interface {
		Put(Indexable) error
	}

	Bound interface {
		Index
		Upper() bool
		Lower() bool
	}
)

type (
	UpperBound struct{ Index }
	LowerBound struct{ Index }
	Where      struct{ Index }
	By         struct{ Index }
)

func (b UpperBound) Upper() bool { return true }
func (b UpperBound) Lower() bool { return false }

func (b LowerBound) Upper() bool { return false }
func (b LowerBound) Lower() bool { return true }

func (b Where) Upper() bool { return true }
func (b Where) Lower() bool { return true }

func (b By) Upper() bool { return false }
func (b By) Lower() bool { return false }

type Page struct {
	Queryable
	Skip  int
	Limit int
}

func (e *Page) AppendBinary(data []byte) (bool, error) {
	if e.Limit > 0 {
		e.Limit--
	}
	if e.Skip > 0 {
		e.Skip--
		return true, nil
	}
	_, err := e.Queryable.AppendBinary(data)
	return e.Limit > 0, err
}

type (
	reader struct {
		*bolt.Tx
	}
	writer struct {
		*bolt.Tx
	}
)

type Count struct {
	Total int
}

func (c *Count) AppendBinary([]byte) error {
	c.Total++
	return nil
}

func NewReader(tx *bolt.Tx) Reader {
	return &reader{tx}
}

func NewWriter(tx *bolt.Tx) Writer {
	return &writer{tx}
}
