package graviton

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var ddd_ = time.Now()

// this creates a persistant store in tmp dir
// then commits and does a number of tests
// the closes the store and reopens and does all the tests again
func TestPersistantStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	store, err := NewDiskStore(dir)
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	var keys = [][]byte{}
	var values = [][]byte{}
	var roothashes = [][HASHSIZE]byte{}

	for i := 0; i < 200; i++ {
		key := make([]byte, 20)
		value := make([]byte, 10)
		rand.Read(key)
		rand.Read(value)
		require.NoError(t, tree.Put(key, value))
		keys = append(keys, key)
		values = append(values, value)

		roothashes = append(roothashes, tree.hashSkipError())
		tree.Commit(fmt.Sprintf("%d", i+1))
	}

	for i, key := range keys {
		value, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, values[i], value)
	}

	current_version := tree.GetVersion()
	for i := uint64(1); i < current_version; i++ {

		gv, err := store.LoadSnapshot(0)
		require.NoError(t, err)

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

	store.Close()

	// lets open the store again, and then lets rerun all the tests
	store, err = NewDiskStore(dir)
	require.NoError(t, err)
	gv, err = store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err = gv.GetTree("root")
	require.NoError(t, err)

	for i, key := range keys {
		value, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, values[i], value)
	}

	current_version = tree.GetVersion()
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

func TestPersistantStore_fail(t *testing.T) {

	// test if root dir cannot be created
	{
		dir, err := ioutil.TempDir("", "example")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up
		tmpfn := filepath.Join(dir, "tmpfile")
		require.NoError(t, ioutil.WriteFile(tmpfn, []byte("dummy"), 0666))
		_, err = NewDiskStore(tmpfn)
		require.Error(t, err)
	}

	// test if version_root cannot be created
	{
		dir, err := ioutil.TempDir("", "example1")
		require.NoError(t, err)
		defer os.RemoveAll(dir)                         // clean up
		tmpfn := filepath.Join(dir, "version_root.bin") // we have created a directory named version root, thus disk stores should not be created
		require.NoError(t, os.MkdirAll(tmpfn, 0700))
		_, err = NewDiskStore(dir)
		require.Error(t, err)
	}

	// test if first file cannot be created, since a directory exists with the name
	{
		dir, err := ioutil.TempDir("", "example2")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up

		s := Store{storage_layer: disk, base_directory: dir}

		tmpfn := s.uint_to_filename(0) // basedir/0/0/0
		require.NoError(t, os.MkdirAll(tmpfn, 0700))

		_, err = NewDiskStore(dir)
		require.Error(t, err)
	}

	// test if first file cannot be created
	{
		dir, err := ioutil.TempDir("", "example3")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up

		s := Store{storage_layer: disk, base_directory: dir}

		tmpfn := filepath.Dir(filepath.Dir(s.uint_to_filename(0))) // basedir/0/0/0
		require.NoError(t, os.MkdirAll(tmpfn, 0700))

		tmpfn = filepath.Dir(filepath.Dir(s.uint_to_filename(0))) // basedir/0/0/0

		require.NoError(t, ioutil.WriteFile(filepath.Join(tmpfn, "0"), []byte("dummy"), 0666))

		_, err = NewDiskStore(dir)

		//t.Logf("err %s",err)
		require.Error(t, err)
	}

	// test if first directory  cannot be created
	{
		dir, err := ioutil.TempDir("", "example4")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up
		require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "0"), []byte("dummy"), 0666))
		_, err = NewDiskStore(dir)
		require.Error(t, err)
	}

	// test if first file cannot be created  cannot be created
	{
		dir, err := ioutil.TempDir("", "example4")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up
		require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "0"), []byte("dummy"), 0666))
		_, err = NewDiskStore(dir)
		require.Error(t, err)
	}

	// test if new writes will create new memory segments
	{
		store, err := NewMemStore()
		store.files[0].size = MAX_FILE_SIZE
		findex, fpos, err := store.write(make([]byte, 512, 512))
		if findex != 1 || fpos != 0 || err != nil {
			t.Fatalf("write failed")
		}

		store.storage_layer = unknown_layer
		store.files[1].size = MAX_FILE_SIZE
		_, _, err = store.write(make([]byte, 512, 512))
		require.Error(t, err)
	}

	{ // test if new writes will create disk segments
		dir, err := ioutil.TempDir("", "example5")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up
		store, err := NewDiskStore(dir)
		require.NoError(t, err)

		store.files[0].size = MAX_FILE_SIZE
		findex, fpos, err := store.write(make([]byte, 512, 512))
		if findex != 1 || fpos != 0 || err != nil {
			t.Fatalf("write failed")
		}
	}

	{ // test if new writes will  give error if new directories could not be created
		//
		dir, err := ioutil.TempDir("", "example67")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up
		store, err := NewDiskStore(dir)
		require.NoError(t, err)

		require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "0", "0", "1"), []byte("dummy"), 0666))

		store.findex = 255

		//t.Logf("filename %s", store.uint_to_filename(256))
		store.files[255] = &file{memoryfile: make([]byte, 512, 512)}
		store.files[255].size = MAX_FILE_SIZE
		_, _, err = store.write(make([]byte, 512, 512))
		require.Error(t, err)
	}

	{ // test if new writes will  give error if new directories could not be created
		//
		dir, err := ioutil.TempDir("", "example88")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up
		store, err := NewDiskStore(dir)
		require.NoError(t, err)

		require.NoError(t, os.MkdirAll(filepath.Join(dir, "0", "0", "1", "256.dfs"), 0700))

		store.findex = 255

		//t.Logf("filename %s", store.uint_to_filename(256))
		store.files[255] = &file{memoryfile: make([]byte, 512, 512)}
		store.files[255].size = MAX_FILE_SIZE
		_, _, err = store.write(make([]byte, 512, 512))
		require.Error(t, err)
	}

	// test if first directory could not be created
	{
		dir, err := ioutil.TempDir("", "example899")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up

		store, err := NewDiskStore(dir)
		require.NoError(t, err)

		os.RemoveAll(filepath.Join(dir, "0"))

		require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "0"), []byte("dummy"), 0666))

		require.Error(t, store.create_first_file())

	}

	// test if first file could not be created
	{
		dir, err := ioutil.TempDir("", "example999")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up

		store, err := NewDiskStore(dir)
		require.NoError(t, err)

		os.RemoveAll(filepath.Join(dir, "0"))

		require.NoError(t, os.MkdirAll(filepath.Join(dir, "0", "0", "0", "0.dfs"), 0700))

		require.Error(t, store.create_first_file())

	}

	// check if version could not be written error is returned

	// test if first file could not be created
	{
		dir, err := ioutil.TempDir("", "example2000")
		require.NoError(t, err)
		defer os.RemoveAll(dir) // clean up

		store, err := NewDiskStore(dir)
		require.NoError(t, err)

		store.versionrootfile.diskfile.Close() // close version file handle to trigger error

		require.Error(t, store.writeVersionData(0, 0, 0))

	}

}

func TestPersistantStore_Empty(t *testing.T) {
	var emptystore Store
	require.Panics(t, func() { emptystore.Close() }) // empty store cannot close

	require.Panics(t, func() { emptystore.uint_to_filename(0) }) // empty store cannot  translate filenames
	emptystore.storage_layer = memory
	require.Panics(t, func() { emptystore.uint_to_filename(0) }) // memory store cannot  translate filenames

	emptystore.storage_layer = unknown_layer
	require.Error(t, emptystore.loadfiles())               // files cannot be loaded from unknown layer
	require.Error(t, emptystore.loadsnapshottablestoram()) // snapshot table cannot eb loaded from unknown layer
	emptystore.storage_layer = memory
	emptystore.versionrootfile = &file{memoryfile: make([]byte, 512, 512)}
	require.NoError(t, emptystore.loadsnapshottablestoram())

	emptystore.storage_layer = unknown_layer
	require.Error(t, emptystore.writeVersionData(0, 0, 0)) // unknown cannot write version data

	//emptystore.storage_layer = memory
	//emptystore.versionrootfile  = &file{}
	//require.Error(t,emptystore.writeVersionData(0,0,0)) // unknown cannot write version data

	emptystore.storage_layer = unknown_layer
	_, err := emptystore.read(0, 0, nil)
	require.Error(t, err) // unknown cannot read data

	store, err := NewMemStore()

	_, err = store.read(0, 2, nil)
	require.Error(t, err) // memory cannot read data more than what is has

	emptystore = Store{}
	_, _, err = emptystore.write([]byte{})
	require.Error(t, err) // empty cannot write data

	emptystore.files = map[uint32]*file{}
	emptystore.files[0] = &file{memoryfile: make([]byte, 512, 512)}
	_, err = emptystore.read(0, 2, nil)
	require.Error(t, err) // empty store cannot read data

}
