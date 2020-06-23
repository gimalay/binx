package binx

import (
	bolt "github.com/coreos/bbolt"
	"github.com/pkg/errors"
)

func (w *Tx) Put(idx Indexable) error {
	tx := w.Tx

	bucket := tx.Bucket(idx.BucketKey())
	if bucket == nil {
		return errors.New("cannot get bucket " + string(idx.BucketKey()))
	}

	err := processIndexable(tx, idx)
	if err != nil {
		return errors.Wrap(err, "process indexes")
	}

	return errors.Wrap(put(bucket, idx), "put")
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

		err = b.Put(idx.UniqueKey(), nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func createMasterIndex(tx *bolt.Tx, mib *bolt.Bucket, idx Indexable) error {
	if ib, err := mib.CreateBucketIfNotExists(idx.UniqueKey()); err == nil {
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
	ib := mib.Bucket(idx.UniqueKey())
	if ib == nil {
		return nil
	}

	err := ib.ForEach(func(k, v []byte) error {
		if b := tx.Bucket(k); b != nil {
			if b2 := b.Bucket(v); b2 != nil {
				return b2.Delete(idx.UniqueKey())
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return mib.DeleteBucket(idx.UniqueKey())
}

func put(b *bolt.Bucket, idx Indexable) (err error) {
	var val []byte

	if val, err = idx.MarshalBinary(); err != nil {
		return errors.Wrap(err, "can't marshal storable")
	}

	return b.Put(idx.UniqueKey(), val)
}
