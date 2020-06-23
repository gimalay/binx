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

	Bound interface {
		Index
		Upper() bool
		Lower() bool
	}
)

func UpperBound(idx Index) Bound                  { return upperBound{idx} }
func LowerBound(idx Index) Bound                  { return lowerBound{idx} }
func Where(idx Index) Bound                       { return where{idx} }
func By(idx Index) Bound                          { return by{idx} }
func Page(q Queryable, skip, limit int) Queryable { return &page{q, skip, limit} }

type Tx struct {
	*bolt.Tx
}

type (
	upperBound struct{ Index }
	lowerBound struct{ Index }
	where      struct{ Index }
	by         struct{ Index }
)

func (b upperBound) Upper() bool { return true }
func (b upperBound) Lower() bool { return false }

func (b lowerBound) Upper() bool { return false }
func (b lowerBound) Lower() bool { return true }

func (b where) Upper() bool { return true }
func (b where) Lower() bool { return true }

func (b by) Upper() bool { return false }
func (b by) Lower() bool { return false }

type page struct {
	Queryable
	Skip  int
	Limit int
}

func (e *page) AppendBinary(data []byte) (bool, error) {
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

type Count struct {
	Total int
}

func (c *Count) AppendBinary([]byte) error {
	c.Total++
	return nil
}
