package graviton

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var dddd_ = time.Now()

// this tests various treename entry points
func TestTreeNameLimit(t *testing.T) {

	store, err := NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	colonname := ":root"
	longname := "11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"

	tree, err := gv.GetTree(colonname)
	require.Error(t, err)
	tree, err = gv.GetTree(longname)
	require.Error(t, err)

	tree, err = gv.GetTreeWithVersion(colonname, 0)
	require.Error(t, err)

	gv.putTreeHighestVersion(colonname, 0)

	_ = tree

}

func TestGetTreeHighestVersion(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	colonname := ":root"

	var faulty_uvarint = [12]byte{0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88}

	gv.vroot.Insert(gv.store, newLeaf(sum([]byte(colonname)), []byte(colonname), faulty_uvarint[:])) // we have inserted faulty data,
	// lets call back and check whether its detected

	_, err = gv.GetTreeHighestVersion("root")
	require.Error(t, err)

	_, _, err = store.loadrootusingpos(0, 1) // no bytes to read and thus error
	require.Error(t, err)                    //

	// more tests
	var faulty_inner = [512]byte{3, 5, 99, 0}
	findex, fpos, err := store.write(faulty_inner[:4])
	require.NoError(t, err)

	encode(findex, fpos, faulty_inner[:]) // inject this into vroot
	gv.vroot.Insert(gv.store, newLeaf(sum([]byte(colonname)), []byte(colonname), faulty_inner[:]))
	// lets call back and check whether its detected
	_, err = gv.loadTree([]byte(colonname))

	require.Error(t, err)

	_, err = gv.loadTree([]byte("treedoesnotexist"))
	require.Error(t, err)

}

/*
func TestIwriteVersionData(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	store.versionrootfile.Close()
	require.Error(t, store.writeVersionData(0, 0, 0))
}
*/

func TestLoadSnapshot(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	_, err = store.LoadSnapshot(99) // trigger version is higher than  available error
	require.Error(t, err)

	//fmt.Printf("error %s\n", store.loadsnapshottablestoram())

	store.version_data[0] = 1
	store.version_data[16] = 1

	_, err = store.LoadSnapshot(1) // trigger recent version corruption
	require.Error(t, err)
	store.version_data[24] = 2
	store.version_data[24+16] = 1
	_, err = store.LoadSnapshot(1) // trigger recent version corruption
	require.Error(t, err)

	_, _, err = store.write([]byte{3, 0, 0, 0}) // write empty inner record
	_, err = store.LoadSnapshot(1)              // trigger recent version corruption
	require.Error(t, err)

	// create a complex error, where deep error is created using internal structures
	store, err = NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	var zerobuf [66000]byte
	tree.Put([]byte{44}, zerobuf[:])
	require.NoError(t, tree.Commit()) // version 1
	tree.Put([]byte{3}, zerobuf[:])   // this also tests a leaf whether large size load for leafs work correctly
	require.NoError(t, tree.Commit()) // version 2

	gv, err = store.LoadSnapshot(0)
	require.NoError(t, err)

	eposition, err := gv.vroot.Get(store, sum([]byte{':', ':', 1}))
	require.NoError(t, err)

	_, fpos := decode(eposition)

	// lets overwrite the file with with corrupt inner node
	file := store.files[0]
	//file.fileh.WriteAt([]byte{3, 5, 99, 0}, int64(fpos))
	copy(file.memoryfile[fpos:], []byte{3, 5, 99, 0})
	_, err = store.LoadSnapshot(1) // trigger recent version corruption
	require.Error(t, err)
}

func TestIloadSnapshottablestoram(t *testing.T) {
	//store, err := NewMemStore()

	dir, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dir) // clean up

	store, err := NewDiskStore(dir) // make file handles unlimited
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)
	store.Close()
	store.version_data_loaded = false

	gv, err = store.LoadSnapshot(1)
	require.Error(t, err) //  this root does not exist

	// second error
	dir2, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)        // clean up
	store, err = NewDiskStore(dir2) // make file handles unlimited
	require.NoError(t, err)
	gv, err = store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	tree.Put([]byte{byte(0)}, []byte{byte(0)})
	require.NoError(t, tree.Commit())
	tree.Put([]byte{byte(1)}, []byte{byte(1)})
	require.NoError(t, tree.Commit())
	store.versionrootfile.diskfile.Truncate(510)
	store.version_data_loaded = false
	gv, err = store.LoadSnapshot(1)
	require.Error(t, err) //  this root does not exist

}
