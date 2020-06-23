package binx

import (
	"bytes"

	bolt "github.com/coreos/bbolt"
	"github.com/pkg/errors"
)

func (r *Tx) Get(q Queryable, key []byte) error {
	tx := r.Tx
	if q == nil {
		return errors.New(errNilPointer)
	}
	if len(key) == 0 {
		return errors.New(errEmptyKey)
	}
	bkt := tx.Bucket(q.BucketKey())
	if bkt == nil {
		panic("cannot get bkt " + string(q.BucketKey()))
	}
	data := bkt.Get(key)
	if data == nil {
		return ErrNotFound
	}

	_, err := q.AppendBinary(data)

	return err
}

func (r *Tx) Scan(s Queryable, bns []Bound) error {
	tx := r.Tx

	if len(bns) == 0 {
		return list(tx, s)
	}

	v := bns

	if len(v) == 1 {

		b := v[0]

		if b.Upper() && b.Lower() {
			return listWhere(tx, s, b)
		}

		if !b.Upper() && !b.Lower() {
			return listBy(tx, s, v[0])
		}

		if b.Upper() {
			return listRange(tx, s, nil, b)
		}

		if b.Lower() {
			return listRange(tx, s, b, nil)
		}

	}

	if len(v) == 2 {

		if v[0].Upper() && v[1].Lower() {
			return listRange(tx, s, v[1], v[0])
		}
		if v[1].Upper() && v[0].Lower() {
			return listRange(tx, s, v[0], v[1])
		}
	}

	return errors.New("Not implemented")
}

func list(r *bolt.Tx, q Queryable) error {
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		return ErrIdxNotFound
	}

	c := bkt.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		more, err := q.AppendBinary(v)
		if !more {
			return nil
		}

		if err != nil {
			return errors.Wrap(err, "failed to unmarshal storable")
		}
	}

	return nil
}

func listBy(r *bolt.Tx, q Queryable, byIdx Bucket) error {
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		return ErrIdxNotFound
	}

	ix := r.Bucket(byIdx.BucketKey())
	if ix == nil {
		return ErrIdxNotFound
	}

	ic := ix.Cursor()

	more := false

	for ik, _ := ic.First(); ik != nil; ik, _ = ic.Next() {

		kb := ix.Bucket(ik)
		kc := kb.Cursor()

		for k, _ := kc.First(); k != nil; k, _ = kc.Next() {
			var err error = nil
			more, err = q.AppendBinary(bkt.Get(k))
			if err != nil {
				return err
			}
			if !more {
				return nil
			}
		}

	}

	return nil
}

func listRange(r *bolt.Tx, q Queryable, from, to Index) error {

	index := from

	if index == nil {
		index = to
	}

	if index == nil {
		return errors.New("cannot build range with nil index")
	}

	if to != nil {
		if !bytes.Equal(index.BucketKey(), to.BucketKey()) {
			return errors.New("cannot build range for two different indexes")
		}
	}
	if from != nil {
		if !bytes.Equal(index.BucketKey(), from.BucketKey()) {
			return errors.New("cannot build range for two different indexes")
		}
	}

	if q == nil {
		return errors.New(errNilPointer)
	}
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		return ErrIdxNotFound
	}

	ix := r.Bucket(index.BucketKey())
	if ix == nil {
		return ErrIdxNotFound
	}

	ic := ix.Cursor()

	s, _ := ic.First()
	if from != nil {
		s, _ = ic.Seek(from.Key())
	}

	for ik := s; ik != nil; ik, _ = ic.Next() {
		if to != nil && bytes.Compare(ik, to.Key()) > 0 {
			break
		}

		kb := ix.Bucket(ik)
		kc := kb.Cursor()

		for k, _ := kc.First(); k != nil; k, _ = kc.Next() {
			more, err := q.AppendBinary(bkt.Get(k))
			if err != nil {
				return err
			}
			if !more {
				return nil
			}
		}
	}

	return nil
}

func listWhere(r *bolt.Tx, q Queryable, index Index) error {
	if q == nil {
		return errors.New(errNilPointer)
	}
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		return ErrIdxNotFound
	}

	ib := r.Bucket(index.BucketKey())
	if ib == nil {
		return ErrIdxNotFound
	}

	ik := ib.Bucket(index.Key())
	if ik == nil {
		return nil
	}

	c := ik.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {

		more, err := q.AppendBinary(bkt.Get(k))
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}

	return nil
}
