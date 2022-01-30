package graviton

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHashPanic(t *testing.T) {
	store, err := NewMemStore()
	//store,err := NewDiskStore("/tmp/test")
	require.NoError(t, err)

	var l leaf
	l.loaded_partial = true

	_, err = l.Hash(store) // it will panic and thus successfully complete the test
	require.Error(t, err)
}

func TestLoadfullleaffromstore(t *testing.T) {
	store, err := NewMemStore()
	//store,err := NewDiskStore("/tmp/test")
	require.NoError(t, err)

	var l leaf
	l.loaded_partial = true

	_, err = l.Get(store, sum([]byte("dummykey"))) // trigger load error for Get
	require.Error(t, err)

	_, _, err = l.Delete(store, sum([]byte("dummykey"))) // trigger load error for Delete
	require.Error(t, err)

	var p Proof
	err = l.Prove(store, sum([]byte("dummykey")), &p) // trigger load error for Prove
	require.Error(t, err)

	err = l.Put(store, sum([]byte("dummykey")), []byte("dummyvalue")) // trigger load error for Get
	require.Error(t, err)

	l.findex = 100000
	err = l.loadfullleaffromstore(store)
	require.Error(t, err)

	keysizeerror := []byte{0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88} // more than 2^64
	l.findex, l.fpos, err = store.write(keysizeerror)
	require.NoError(t, err)
	err = l.loadfullleaffromstore(store) // loading it will give keysize error
	require.Error(t, err)

	valuesizeerror := []byte{0x1, 0x0, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88} // more than 2^64
	l.findex, l.fpos, err = store.write(valuesizeerror)
	require.NoError(t, err)
	err = l.loadfullleaffromstore(store) // loading it will give value size error
	require.Error(t, err)

}
