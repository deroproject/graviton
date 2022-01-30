package graviton

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setupDeterministicTree(tb testing.TB, keycount int) (*Store, *Tree) {
	rand.Seed(100)
	store, err := NewMemStore()
	require.NoError(tb, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(tb, err)
	tree, err := gv.GetTree("root")
	require.NoError(tb, err)
	for i := 0; i < keycount; i++ {
		key := make([]byte, 50)
		value := make([]byte, 60)
		rand.Read(key)
		rand.Read(value)
		require.NoError(tb, tree.Put(key, value))
	}
	if keycount > 0 {
		require.NoError(tb, tree.Commit())
	}
	return store, tree
}

func (t *Tree) hashSkipError() [HASHSIZE]byte {
	h, _ := t.Hash()
	return h
}

/*
func (in *inner) clean_hash(store *Store) { //marks an entire tree as dirty, inclusing leaves, causes to load entiire tree to ram

	if in.loaded_partial { // if leaf is loaded partially, load it fully now
		if err := in.loadinnerfromstore(store); err != nil {
			panic(err)
		}
	}

	switch tmp := in.right.(type) { // if right node is not dead end
	case nil:
	case *inner: // if its inner node, recursively insert  the node
		tmp.clean_hash(store)
	case *leaf: // below case inserts or overwrites existing value, which dropping chains  of long length
		tmp.clean_hash(store)
	}

	switch tmp := in.left.(type) { // if right node is not dead end
	case nil:
	case *inner: // if its inner node, recursively insert  the node
		tmp.clean_hash(store)
	case *leaf: // below case inserts or overwrites existing value, which dropping chains  of long length
		tmp.clean_hash(store)
	}


	in.hash = in.hash[:0]
}

func (l *leaf) clean_hash(store *Store) {
	if l.loaded_partial { // if leaf is loaded partially, load it fully now
		if err := l.loadfullleaffromstore(store); err != nil {
			panic("invalid hash " + fmt.Sprintf("err %s", err))
		}
	}

	for i := range l.hash {
		l.hash[i] = 0xff
	}
}
*/

func TestTreeMaxValueSize(t *testing.T) {
	store, err := NewMemStore()
	//store, err := NewDiskStore("/tmp/test") // make file handles unlimited
	require.NoError(t, err)

	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")

	dummyvalue := make([]byte, MAX_VALUE_SIZE+1, MAX_VALUE_SIZE+1)
	key := make([]byte, 40)
	rand.Read(key)

	require.Error(t, tree.Put(key, dummyvalue))
}

func TestTreePutGetDelete(t *testing.T) {
	rand.Seed(time.Now().Unix())
	store, err := NewMemStore()
	//store,err := NewDiskStore("/tmp/test")
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)
	var keys = [][]byte{}
	var values = [][]byte{}
	var roothashes = [][HASHSIZE]byte{}

	for i := 0; i < 500; i++ {
		key := []byte(randStr(20))
		value := []byte(randStr(10))
		require.NoError(t, tree.Put(key, value))
		keys = append(keys, key)
		values = append(values, value)

		roothashes = append(roothashes, tree.hashSkipError())
		tree.Commit()

		// since the test is single threaded, version number should be monotonicaly increasing
		require.Equal(t, tree.GetVersion(), tree.GetParentVersion()+1)

	}

	for i, key := range keys {
		value, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, values[i], value)
	}

	// delete the keys in reverse order and check whether we obtain the same tree root hash as before
	for i := len(keys) - 1; i > 0; i-- {
		tree.Delete(keys[i])
		require.Equal(t, tree.hashSkipError(), roothashes[i-1])
		tree.Commit() // also check whether commit cause any hash to change
		require.Equal(t, tree.hashSkipError(), roothashes[i-1])
	}

}

func TestArbitraryNonExistingGetsDeletes(t *testing.T) {
	_, tree := setupDeterministicTree(t, 10000)

	for i := 0; i < 1000; i++ {
		key := make([]byte, 50)
		rand.Read(key)
		require.NoError(t, tree.Delete(key))

		value, err := tree.Get(key)
		_ = value
		require.Error(t, err) // all gets must fail
	}
}

//
func TestTreeVersionLabelRootHash(t *testing.T) {
	//rand.Seed(time.Now().Unix())
	rand.Seed(101)
	store, err := NewMemStore()
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)
	var keys = [][]byte{}
	var values = [][]byte{}
	var roothashes = [][HASHSIZE]byte{}

	for i := 0; i < 20; i++ {
		key := make([]byte, 20)
		value := make([]byte, 10)
		rand.Read(key)
		rand.Read(value)
		require.NoError(t, tree.Put(key, value))
		keys = append(keys, key)
		values = append(values, value)

		h := tree.hashSkipError()
		roothashes = append(roothashes, h)
		tree.Commit(fmt.Sprintf("%d", i+1))
	}

	for i, key := range keys {
		value, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, values[i], value)
	}

	gv, err = store.LoadSnapshot(0) // we need to reload global version
	require.NoError(t, err)

	current_version := tree.GetVersion()
	for i := uint64(1); i < current_version; i++ {

		tree, err := gv.GetTreeWithVersion("root", i)
		require.NoError(t, err)
		require.Equal(t, roothashes[i-1], tree.hashSkipError())

		rhash := tree.hashSkipError()
		tree, err = gv.GetTreeWithRootHash(rhash[:]) // reload snapshot using rooted tree
		require.NoError(t, err)
		require.Equal(t, roothashes[i-1], tree.hashSkipError())

		tree, err = gv.GetTreeWithTag(fmt.Sprintf("%d", i)) // reload snapshot using label
		require.NoError(t, err)
		require.Equal(t, roothashes[i-1], tree.hashSkipError())
	}
}

func TestDiscard(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	store, err := NewDiskStore(dir)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	tree.Put([]byte{45}, []byte{89}) // tree is dirty now
	require.Equal(t, true, tree.IsDirty())

	tree.Commit()

	tree.Put([]byte{46}, []byte{89}) // tree is dirty now
	require.Equal(t, true, tree.IsDirty())
	require.NoError(t, tree.Discard())
	require.Equal(t, false, tree.IsDirty()) // tree should not be dirty now

	tree.Put([]byte{46}, []byte{89}) // tree is dirty now
	require.Equal(t, true, tree.IsDirty())

	store.versionrootfile.diskfile.Truncate(0)
	require.Error(t, tree.Discard())

}

// makes sure only dirty trees are committed
func TestCommitDirty(t *testing.T) {

	store, err := NewMemStore()
	//store, err := NewDiskStore("/tmp/test") // make file handles are unlimited
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	tree.Put([]byte{44}, []byte{80})

	require.NoError(t, tree.Commit())

	tree.Put([]byte{45}, []byte{80})
	require.NoError(t, tree.Commit())

	current_version := tree.GetVersion()
	require.NoError(t, tree.Commit())

	require.Equal(t, current_version, tree.GetVersion())
}

// makes sure only dirty trees are committed
func TestCommits(t *testing.T) {

	dir, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	store, err := NewDiskStore(dir)
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	_, err = Commit()
	require.NoError(t, err)

	tree.Put([]byte{44}, []byte{80})
	require.NoError(t, tree.Commit())

	//create err
	//store.versionrootfile.diskfile.Truncate(510) // version file has been damaged
	//store.version_data_loaded = false

	//tree.Put([]byte{45}, []byte{80})
	//require.Error(t, tree.Commit())

}

// a rare case is tested
func TestCommits_rarecase(t *testing.T) {

	dir, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	store, err := NewDiskStore(dir)
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	_, err = Commit()
	require.NoError(t, err)

	//tree.Put([]byte{44}, []byte{80})

	tree.treename = "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	//tree.Put([]byte{45}, []byte{80})

	//create err
	//store.versionrootfile.diskfile.Truncate(510) // version file has been damaged
	//store.version_data_loaded = false

	//_, _, err = tree.commit_inner(gv, false, 0, tree.root)
	//require.Error(t, err)

	// this is to test a condition which will probably never occur until disk is corrupted

}

func TestSnapshotGets(t *testing.T) {
	store, err := NewMemStore()
	//store, err := NewDiskStore("/tmp/test") // make file handles are unlimited
	require.NoError(t, err)

	gv0, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree, err := gv0.GetTree("root")
	require.NoError(t, err)
	for i := 0; i < 25; i++ {
		for j := 0; j < 10; j++ {
			tree.Put([]byte{byte(i*10 + j)}, []byte{byte(i*10 + j)})
		}
		require.NoError(t, tree.Commit())
		require.Equal(t, uint64(i+1), tree.GetVersion())
		require.Equal(t, uint64(i), tree.GetParentVersion())

	}

	for i := 1; i <= 25; i++ {
		gv, err := store.LoadSnapshot(uint64(i))
		require.NoError(t, err)
		tree, err := gv.GetTree("root")
		require.NoError(t, err)

		for j := 0; j < 250; j++ {
			_, err := tree.Get([]byte{byte(j)})
			if j < (i)*10 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		}
	}
}

func TestSnapshotMultiGets(t *testing.T) {
	store, err := NewMemStore()
	//store, err := NewDiskStore("/tmp/test") // make file handles are unlimited
	require.NoError(t, err)

	gv0, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree1, err := gv0.GetTree("root1")
	require.NoError(t, err)
	tree2, err := gv0.GetTree("root2")
	require.NoError(t, err)
	for i := 0; i < 25; i++ {
		for j := 0; j < 10; j++ {
			tree1.Put([]byte{byte(i*10 + j)}, []byte{byte(i*10 + j)})
			tree2.Put([]byte{byte(i*10 + j + 1)}, []byte{byte(i*10 + j)}) // key is 1 higher than value

		}
		commit_version, err := Commit(tree1, tree2) // commit both trees
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), commit_version) // commit version is monotonically increasing version
		require.Equal(t, uint64(i+1), tree1.GetVersion())
		require.Equal(t, uint64(i), tree1.GetParentVersion())
		require.Equal(t, uint64(i+1), tree2.GetVersion())
		require.Equal(t, uint64(i), tree2.GetParentVersion())

		gv1, err := store.LoadSnapshot(0)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), gv1.GetVersion())

	}

	for i := 1; i <= 25; i++ {
		gv, err := store.LoadSnapshot(uint64(i))
		require.NoError(t, err)
		tree1, err := gv.GetTree("root1")
		require.NoError(t, err)
		tree2, err := gv.GetTree("root2")
		require.NoError(t, err)

		for j := 0; j < 250; j++ {
			_, err := tree1.Get([]byte{byte(j)})
			if j < (i)*10 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}

			val, err := tree2.Get([]byte{byte(j + 1)})
			if j < (i)*10 {
				require.NoError(t, err)
				if len(val) != 1 || val[0] != byte(j) {
					t.Fatalf("value failed")
				}
			} else {
				require.Error(t, err)
			}
		}
	}
}

func TestSnapshotGetsDeletes(t *testing.T) {
	store, err := NewMemStore()
	//store, err := NewDiskStore("/tmp/test") // make file handles unlimited
	require.NoError(t, err)

	gv0, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree, err := gv0.GetTree("root")
	require.NoError(t, err)
	for i := 0; i < 25; i++ {
		for j := 0; j < 255; j++ {
			if j >= i*10 && j < (i+1)*10 {
				tree.Put([]byte{byte(j)}, []byte{byte(j)})
			} else if j < i*10 {
				tree.Delete([]byte{byte(j)}) // delete early keys, all commits will only contain 10 keys
			}
		}
		require.NoError(t, tree.Commit())

		require.Equal(t, uint64(i+1), tree.GetVersion())
		require.Equal(t, uint64(i), tree.GetParentVersion())

	}

	for i := 1; i <= 25; i++ {
		gv, err := store.LoadSnapshot(uint64(i))
		require.NoError(t, err)
		tree, err := gv.GetTree("root")
		require.NoError(t, err)

		for j := 0; j < 250; j++ {
			value, err := tree.Get([]byte{byte(j)})
			if j >= (i-1)*10 && j < (i)*10 {
				require.NoError(t, err) // check gets
				require.Equal(t, value, []byte{byte(j)})
			} else {
				require.Error(t, err) // check deletes
			}
		}
	}
}

func TestCommitinner(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	tree.Put([]byte{44}, []byte{80})
	tree.Put([]byte{45}, []byte{81})
	tree.Put([]byte{46}, []byte{82})
	tree.root.left = &dummynode{dirty: true}
	require.Error(t, tree.Commit())
	tree.root.left = nil
	tree.root.right = &dummynode{dirty: true}
	require.Error(t, tree.Commit())
	tree.root.right = nil

	store.Close()

	_, _, err = tree.commit_inner(gv, false, 0, tree.root)
	require.Error(t, err)
}

func TestMultiCommits(t *testing.T) {
	store, err := NewMemStore()
	//store, err := NewDiskStore("/tmp/test") // make file handles are unlimited
	require.NoError(t, err)

	gv0, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree1, err := gv0.GetTree("root1")
	require.NoError(t, err)
	tree2, err := gv0.GetTree("root2")
	require.NoError(t, err)
	tree1.Put([]byte{byte(1)}, []byte{byte(1)})
	tree2.Put([]byte{byte(2)}, []byte{byte(2)})
	_, err = Commit(tree1, tree2) // commit both trees
	require.NoError(t, err)

	tree1.Put([]byte{byte(1)}, []byte{byte(1)})
	tree2.Put([]byte{byte(2)}, []byte{byte(2)})
	_, err = Commit(tree1, tree2) // commit both trees
	require.NoError(t, err)

	// now lets load the same tree from different snapshots

	gv1, err := store.LoadSnapshot(1)
	require.NoError(t, err)

	gv2, err := store.LoadSnapshot(2)
	require.NoError(t, err)

	tree1, err = gv1.GetTree("root1")
	require.NoError(t, err)
	tree2, err = gv2.GetTree("root2")
	require.NoError(t, err)

	tree1.Put([]byte{byte(1)}, []byte{byte(1)})
	tree2.Put([]byte{byte(2)}, []byte{byte(2)})

	_, err = Commit(tree1, tree2) // commit both trees but an error since both trees are from different snapshot
	require.Error(t, err)

	tree1.snapshot_version = 3 // non existant version
	err = tree1.Commit()
	require.Error(t, err)

}
