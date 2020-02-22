package binx

import (
	bolt "github.com/coreos/bbolt"
	"github.com/pkg/errors"
)

type (
	writer struct {
		*bolt.Tx
		*reader
	}
)

func (w *writer) Put(s Indexable) error {
	tx := w.Tx

	bucket := tx.Bucket(s.BucketKey())
	if bucket == nil {
		return errors.New("cannot get bucket " + string(s.BucketKey()))
	}

	err := processIndexable(tx, s)
	if err != nil {
		return errors.Wrap(err, "process indexes")
	}

	return errors.Wrap(put(bucket, s), "put")
}

func processIndexable(tx *bolt.Tx, idx Indexable) error {
	bkt := tx.Bucket(idx.MasterIndexBucketKey())
	if bkt == nil {
		return errors.New("master index bucket cannot be found")
	}
	err := cleanupIndexes(tx, bkt, idx)
	if err != nil {
		return errors.Wrap(err, "cleanup indexes")
	}

	err = createIndexes(tx, idx)
	if err != nil {
		return errors.Wrap(err, "create indexes")
	}
	return createMasterIndex(tx, bkt, idx)
}

func createIndexes(tx *bolt.Tx, idx Indexable) error {
	for _, i := range idx.Indexes() {
		idxBkt := tx.Bucket(i.BucketKey())
		if idxBkt == nil {
			return errors.Errorf("index bucket not found %v", string(i.BucketKey()))
		}

		if len(i.Key()) == 0 {
			return errors.Errorf("index %v key cannot be empty", string(i.BucketKey()))
		}

		b, err := idxBkt.CreateBucketIfNotExists(i.Key())
		if err != nil {
			return err
		}

		err = b.Put(idx.Key(), nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func createMasterIndex(tx *bolt.Tx, mib *bolt.Bucket, idx Indexable) error {
	if ib, err := mib.CreateBucketIfNotExists(idx.Key()); err == nil {
		for _, i := range idx.Indexes() {
			err := ib.Put(i.BucketKey(), i.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func cleanupIndexes(tx *bolt.Tx, mib *bolt.Bucket, idx Indexable) error {
	ib := mib.Bucket(idx.Key())
	if ib == nil {
		return nil
	}

	err := ib.ForEach(func(k, v []byte) error {
		if b := tx.Bucket(k); b != nil {
			if b2 := b.Bucket(v); b2 != nil {
				return b2.Delete(idx.Key())
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return mib.DeleteBucket(idx.Key())
}

func put(bucket *bolt.Bucket, index Index) (err error) {
	var val []byte

	if storable, ok := index.(Storable); ok {
		if val, err = storable.MarshalBinary(); err != nil {
			return errors.Wrap(err, "can't marshal storable")
		}
	}

	return bucket.Put(index.Key(), val)
}
