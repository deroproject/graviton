package graviton

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInnerUnMarshal(t *testing.T) {
	var buffs = [][]byte{
		[]byte{0, 0}, // small buf error
		[]byte{24, innerNODE, 0, 0, 0, 0, 0x88, 0x88, 0x88, 0x88, 0x88, 0x8},       // more than 4 billion findex left node
		[]byte{24, innerNODE, 0, 0, 0, 0, 0x10, 0x88, 0x88, 0x88, 0x88, 0x88, 0x8}, // more than 4 billion fpos left node
		[]byte{24, innerNODE, 0, 0, 0, 0, 0x10, 0x10, 0x11, 0x11},                  //  buffer doesn't contain hash size, left

		[]byte{24, 0, leafNODE, 0, 0, 0, 0x88, 0x88, 0x88, 0x88, 0x88, 0x8},       // more than 4 billion findex left node
		[]byte{24, 0, leafNODE, 0, 0, 0, 0x10, 0x88, 0x88, 0x88, 0x88, 0x88, 0x8}, // more than 4 billion fpos left node
		[]byte{24, 0, leafNODE, 0, 0, 0, 0x10, 0x10, 0x11, 0x11},                  //  buffer doesn't contain hash size
		[]byte{24, 0, 3, 0, 0, 0, 0x10, 0x10, 0x11, 0x11},                         //  unknown node type

	}
	var in inner
	for _, buf := range buffs {
		err := in.Unmarshal(buf)
		require.Error(t, err)
	}
}

func TestIsOnlyChildleafPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	if n, ok := isOnlyChildleaf(nil); n != nil || ok != false { // result should be nil, false
		t.Errorf("isOnlyChildleaf failed for nil")
	}

	if n, ok := isOnlyChildleaf(&leaf{}); n != nil || ok != false { // result should be nil, false
		t.Errorf("isOnlyChildleaf failed for leaf")
	}

	isOnlyChildleaf(&dummynode{}) // it will panic and thus successfully complete the test
}

func TestLoadinnerfromstore(t *testing.T) {
	store, err := NewMemStore()
	//store,err := NewDiskStore("/tmp/test")
	require.NoError(t, err)

	var in inner
	require.Error(t, in.loadinnerfromstore(store))

	//in.fpos = 10000
	//require.Error(t, in.loadinnerfromstore(store)) // invalid position

	in.findex = 1000000
	require.Error(t, in.loadinnerfromstore(store)) // invalid findex

	in.loaded_partial = true
	if hash, _ := in.Hash(store); hash != nil { // invalid findex for hash
		t.Fatalf("Hash inner node loading failed")
	}

	_, err = in.Get(store, sum([]byte("dummykey"))) // trigger load error for Get
	require.Error(t, err)

	var l leaf
	err = in.Insert(store, &l) // trigger load error for Insert
	require.Error(t, err)

	//var p Proof
	//err = in.Prove(store, sum([]byte("dummykey")), &p) // trigger load error for Prove
	//require.Error(t, err)

	_, _, err = in.Delete(store, sum([]byte("dummykey"))) // trigger load error for Delete
	require.Error(t, err)

	{ // n level deep Delete errors are simulated here
		var in, inlevel1 inner
		in.loaded_partial = false
		inlevel1.loaded_partial = true
		in.left = &inlevel1
		in.findex = 0
		_, _, err = in.Delete(store, [HASHSIZE]byte{0}) // trigger load error for Delete
		require.Error(t, err)

		in.right = &inlevel1
		_, _, err = in.Delete(store, [HASHSIZE]byte{0x80}) // trigger load error for Delete
		require.Error(t, err)
	}

	{ // n level deep insert errors are simulated here
		var in inner
		l := &leaf{loaded_partial: true, keyhash: [HASHSIZE]byte{0x00, 1, 3}}
		l2 := &leaf{loaded_partial: true, keyhash: [HASHSIZE]byte{0x00, 1, 3, 4}}

		r := &leaf{loaded_partial: true, keyhash: [HASHSIZE]byte{0x80, 1, 3}}
		r2 := &leaf{loaded_partial: true, keyhash: [HASHSIZE]byte{0x80, 1, 3, 4}}

		in.left = l
		err = in.Insert(store, l2) // trigger load error for Insert
		require.Error(t, err)

		in.right = r
		err = in.Insert(store, r2) // trigger load error for  right Insert
		require.Error(t, err)

	}

}

func TestInnerCoverage(t *testing.T) {
	_, tree := setupDeterministicTree(t, 0)
	value := []byte("testdeadend")

	order := []byte{
		0x0,  //00000000,
		0xc0, //11000000,
		0xd0, //11010000,
		0x8,  //10000000,
	}
	for i := range order {
		key := [HASHSIZE]byte{}
		key[0] = order[i]
		require.NoError(t, tree.putRaw(key, nil, value))
	}

	require.Error(t, tree.Commit()) // actually an error should be reported due to corruption

}
