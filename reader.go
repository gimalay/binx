package binx

import (
	"reflect"

	bolt "github.com/coreos/bbolt"
	"github.com/pkg/errors"
)

type (
	reader struct {
		*bolt.Tx
	}
)

func (r *reader) List(q QueryableSlice, limit, skip int) (err error) {
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

func (r *reader) ListBy(q QueryableSlice, index Bucket, limit, skip int) (err error) {
	if q == nil {
		return errors.New(errNilPointer)
	}
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		return ErrIdxNotFound
	}

	idxBkt := r.Bucket(index.BucketKey())
	if idxBkt == nil {
		return ErrIdxNotFound
	}

	ic := idxBkt.Cursor()
	n := 0
	l := 0
	for ivk, _ := ic.First(); ivk != nil; ivk, _ = ic.Next() {

		keyBkt := idxBkt.Bucket(ivk)
		kc := keyBkt.Cursor()

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

func (r *reader) ListWhere(q QueryableSlice, index Index, limit, skip int) (err error) {
	if q == nil {
		return errors.New(errNilPointer)
	}
	bkt := r.Bucket(q.BucketKey())
	if bkt == nil {
		return ErrIdxNotFound
	}

	idxBkt := r.Bucket(index.BucketKey())
	if idxBkt == nil {
		return ErrIdxNotFound
	}

	keyBkt := idxBkt.Bucket(index.Key())
	if keyBkt == nil {
		return nil
	}

	c := keyBkt.Cursor()
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
