package graviton

import (
	"errors"
	"testing"
)

// used to test certain code paths, which though can be avoid now but may arise when code is maintained, developed, rewritten over a period of time
type dummynode struct {
	dirty bool
}

func (d *dummynode) Hash(store *Store) ([]byte, error) {
	var h [HASHSIZE_BYTES]byte
	return h[:], nil
}

func (d *dummynode) isDirty() bool {
	return d.dirty
}

func (d *dummynode) load_partial(store *Store) error {
	return nil
}

func (d *dummynode) Position() (uint32, uint32) {
	return 0, 0
}

func (d *dummynode) Put(store *Store, keyhash [HASHSIZE]byte, value []byte) error {
	return errors.New("not implemented")
}

func (d *dummynode) Get(store *Store, keyhash [HASHSIZE]byte) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (d *dummynode) Delete(store *Store, keyhash [HASHSIZE]byte) (bool, bool, error) {
	return false, false, errors.New("not implemented")
}

func (d *dummynode) Prove(store *Store, keyhash [HASHSIZE]byte, proof *Proof) error {
	return errors.New("not implemented")
}

func TestUnknownNodePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	var d dummynode

	// The following is the code under test
	getNodeType(&d)
}
