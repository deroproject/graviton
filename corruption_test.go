package graviton

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// this creates a persistant store in tmp dir
// then commits and does a number of tests
// the closes the store and reopens and does all the tests again
func TestCorruptionStore_basic(t *testing.T) {

	key := []byte("key1key1key1key1key1key1key1key1key1key1key1key1key1key1")
	value := []byte("This value is good") // an 100 KB value, which will be corrupted later on disk to showcase detection
	value_corrupted := []byte("Corrupted value !!")

	dir, err := ioutil.TempDir("", "graviton_showcase_")
	require.NoError(t, err)

	defer os.RemoveAll(dir) // clean up

	store, err := NewDiskStore(dir)
	require.NoError(t, err)

	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("treename")
	require.NoError(t, err)

	err = tree.Put(key, value)
	require.NoError(t, err)

	_, err = Commit(tree)
	require.NoError(t, err)

	gv, err = store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err = gv.GetTree("treename")
	require.NoError(t, err)
	value, err = tree.Get(key)

	//fmt.Printf("Reading value before corruption \"%s\" err %s\n", string(value), err)

	// now the key has been persisted and verified now we will corrupt the value
	file, err := os.OpenFile(filepath.Join(dir, "0", "0", "0", "0.dfs"), os.O_RDWR, 0644)
	require.NoError(t, err)

	file_contents, err := ioutil.ReadFile(filepath.Join(dir, "0", "0", "0", "0.dfs"))
	require.NoError(t, err)

	index := bytes.Index(file_contents, value)
	if index < 0 {
		panic("err could not find value in DB")
	}

	//fmt.Printf("contain count %d\n", bytes.Count(file_contents,value))

	file.WriteAt(value_corrupted, int64(index))
	file.Close()

	gv, err = store.LoadSnapshot(0)
	require.NoError(t, err)
	tree1, err := gv.GetTree("treename")
	require.NoError(t, err)
	_, err = tree1.Get(key)
	require.Error(t, err)

	dummytree, err := gv.GetTree("dummytree")
	require.NoError(t, err)
	require.Error(t, Diff(tree1, dummytree, nil, nil, nil))

}

func TestCorruptionStore_Left(t *testing.T) {

	key := []byte("key1key1key1key1key1key1key1key1key1key1key1key1key1key1")
	value := []byte("This value is good") // an 100 KB value, which will be corrupted later on disk to showcase detection
	value_corrupted := []byte("Corrupted value !!")

	dir, err := ioutil.TempDir("", "graviton_showcase1_")
	require.NoError(t, err)

	defer os.RemoveAll(dir) // clean up

	store, err := NewDiskStore(dir)
	require.NoError(t, err)

	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("treename")
	require.NoError(t, err)

	for i := 0; i < 8; i++ {
		key1 := randStr(60)
		value1 := randStr(512)
		tree.Put([]byte(key1), []byte(value1))
	}
	err = tree.Put(key, value)
	require.NoError(t, err)
	tree.Commit() // commit the tree

	gv, err = store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err = gv.GetTree("treename")
	require.NoError(t, err)
	value, err = tree.Get(key)

	//fmt.Printf("Reading value before corruption \"%s\" err %s\n", string(value), err)

	// now the key has been persisted and verified now we will corrupt the value
	file, err := os.OpenFile(filepath.Join(dir, "0", "0", "0", "0.dfs"), os.O_RDWR, 0644)
	require.NoError(t, err)

	file_contents, err := ioutil.ReadFile(filepath.Join(dir, "0", "0", "0", "0.dfs"))
	require.NoError(t, err)

	index := bytes.Index(file_contents, value)
	if index < 0 {
		panic("err could not find value in DB")
	}

	//fmt.Printf("contain count %d\n", bytes.Count(file_contents,value))

	file.WriteAt(value_corrupted, int64(index))
	file.Close()

	gv, err = store.LoadSnapshot(0)
	require.NoError(t, err)
	tree1, err := gv.GetTree("treename")
	require.NoError(t, err)
	_, err = tree1.Get(key)
	require.Error(t, err)

	dummytree, err := gv.GetTree("dummytree")
	require.NoError(t, err)
	require.Error(t, Diff(tree1, dummytree, nil, nil, nil))

}
