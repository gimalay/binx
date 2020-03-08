package binx

import (
	"bytes"
	"reflect"

	bolt "github.com/coreos/bbolt"
	"github.com/pkg/errors"
)

type (
	reader struct {
		*bolt.Tx
	}
)

func (r *reader) List(s QueryableSlice) (err error) {
	return r.list(s, 0, 0)
}
func (r *reader) Query(s QueryableSlice, qr Query) error {
	err := qr.validate()
	if err != nil {
		return err
	}

	switch q := qr.(type) {
	case By:
		return r.listBy(s, q.Index, q.Limit, q.Skip)
	case Where:
		return r.listWhere(s, q.Value, q.Limit, q.Skip)
	case Page:
		return r.list(s, q.Limit, q.Skip)
	case Range:
		if q.From != nil {
			return r.listRange(s, q.From, q.From, q.To, q.Limit, q.Skip)
		}

		if q.To != nil {
			return r.listRange(s, q.To, q.From, q.To, q.Limit, q.Skip)
		}

		return r.list(s, q.Limit, q.Skip)
	}

	return errors.New("unknow query")
}

func (r *reader) list(q QueryableSlice, limit, skip int) (err error) {
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		return ErrIdxNotFound
	}

	c := bkt.Cursor()
	n := 0
	l := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		n++
		if skip > 0 && n <= skip {
			continue
		}

		if err = q.AppendBinary(v); err != nil {
			return errors.Wrap(err, "failed to unmarshal storable")
		}

		l++
		if limit > 0 && l >= limit {
			break
		}
	}

	return err
}

func (r *reader) listBy(q QueryableSlice, index Bucket, limit, skip int) (err error) {
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
	n := 0
	l := 0
	for ik, _ := ic.First(); ik != nil; ik, _ = ic.Next() {

		kb := ix.Bucket(ik)
		kc := kb.Cursor()

		for k, _ := kc.First(); k != nil; k, _ = kc.Next() {
			n++
			if skip > 0 && n <= skip {
				continue
			}

			if err = q.AppendBinary(bkt.Get(k)); err != nil {
				return err
			}

			l++
			if limit > 0 && l >= limit {
				return nil
			}
		}

	}

	return err
}

func (r *reader) listRange(q QueryableSlice, index, from, to Index, limit, skip int) error {

	if index == nil {
		return errors.New("cannot build range with nil index")
	}

	if to != nil {
		if !bytes.Equal(index.BucketKey(), to.BucketKey()) {
			return errors.New("cannot build range with two different indexes")
		}
	}
	if from != nil {
		if !bytes.Equal(index.BucketKey(), from.BucketKey()) {
			return errors.New("cannot build range with two different indexes")
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
	n := 0
	l := 0

	s, _ := ic.First()
	if from != nil {
		s, _ = ic.Seek(from.Key())
	}

	for ik := s; ik != nil && (to == nil || bytes.Compare(ik, to.Key()) <= 0); ik, _ = ic.Next() {

		kb := ix.Bucket(ik)
		kc := kb.Cursor()

		for k, _ := kc.First(); k != nil; k, _ = kc.Next() {
			n++
			if skip > 0 && n <= skip {
				continue
			}

			if err := q.AppendBinary(bkt.Get(k)); err != nil {
				return err
			}

			l++
			if limit > 0 && l >= limit {
				return nil
			}
		}
	}

	return nil
}

func (r *reader) listWhere(q QueryableSlice, index Index, limit, skip int) (err error) {
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
	n := 0
	l := 0
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		n++
		if skip > 0 && n <= skip {
			continue
		}

		if err = q.AppendBinary(bkt.Get(k)); err != nil {
			return err
		}

		l++
		if limit > 0 && l >= limit {
			return nil
		}
	}

	return err
}

func (r *reader) First(q Queryable) (err error) {
	if reflect.ValueOf(q).IsNil() {
		return errors.New(errNilPointer)
	}
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		panic("cannot get bkt " + string(q.BucketKey()))
	}
	c := bkt.Cursor()
	_, val := c.First()
	if val == nil {
		return ErrNotFound
	}

	return q.UnmarshalBinary(val)
}

func (r *reader) Last(q Queryable) (err error) {
	if reflect.ValueOf(q).IsNil() {
		return errors.New(errNilPointer)
	}
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		panic("cannot get bkt " + string(q.BucketKey()))
	}
	c := bkt.Cursor()
	_, val := c.Last()
	if val == nil {
		return ErrNotFound
	}

	return q.UnmarshalBinary(val)
}

func (r *reader) Get(q Queryable, key []byte) (err error) {
	if reflect.ValueOf(q).IsNil() {
		return errors.New(errNilPointer)
	}
	if len(key) == 0 {
		return errors.New(errEmptyKey)
	}
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		panic("cannot get bkt " + string(q.BucketKey()))
	}
	data := bkt.Get(key)
	if data == nil {
		return ErrNotFound
	}

	return q.UnmarshalBinary(data)
}
