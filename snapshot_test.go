package graviton

import (
	"fmt"
	//	"io/ioutil"
	//	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var dddd_ = time.Now()

// this tests the version tree dag
// this loading of versions and whether they can be accessed and then moved forward
func TestSnapshotDAG(t *testing.T) {

	store, err := NewMemStore()
	require.NoError(t, err)

	loop_count := uint64(5)
	// now lets commit the tree 5 times
	for i := uint64(0); i < loop_count; i++ {

		gv, err := store.LoadSnapshot(0)
		require.NoError(t, err)

		tree, err := gv.GetTree("root")
		require.NoError(t, err)
		require.Equal(t, i, tree.snapshot_version)

		key := []byte(fmt.Sprintf("%d", i+1))
		value := []byte(fmt.Sprintf("%d", i+1))
		require.NoError(t, tree.Put(key, value))

		commit_version, err := Commit(tree)
		require.NoError(t, err)
		require.Equal(t, i+1, commit_version)
		require.Equal(t, i+1, tree.snapshot_version)
		require.Equal(t, i+1, tree.GetVersion())

	}

	for i := uint64(0); i < loop_count; i++ {

		gv, err := store.LoadSnapshot(i + 1)
		require.NoError(t, err)

		tree, err := gv.GetTree("root")
		require.NoError(t, err)

		for j := uint64(0); j < i; j++ {
			key := []byte(fmt.Sprintf("%d", j+1))
			value := []byte(fmt.Sprintf("%d", j+1))

			value_actual, err := tree.Get(key)
			if err != nil {
				fmt.Printf("value result failed j %d\n", j)
			}
			require.NoError(t, err)
			require.Equal(t, value, value_actual)
		}
	}

	gv, err := store.LoadSnapshot(5)
	require.NoError(t, err)

	highest_version, err := gv.GetTreeHighestVersion("root")
	require.NoError(t, err)
	require.Equal(t, uint64(5), highest_version)

	// now lets test we can move past in history
	gv, err = store.LoadSnapshot(3)
	require.NoError(t, err)

	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	key := []byte(fmt.Sprintf("%d", 4))
	value := []byte(fmt.Sprintf("%d", 4))
	require.NoError(t, tree.Put(key, value))
	commit_version, err := Commit(tree)
	require.NoError(t, err)
	require.Equal(t, uint64(6), commit_version) // 5 version committed earlier
	require.Equal(t, uint64(6), tree.snapshot_version)
	require.Equal(t, uint64(4), tree.GetVersion()) // tree version should be 4

}

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

	// now lets commit the tree 5 times
	loop_count := uint64(5)
	for i := uint64(0); i < loop_count; i++ {
		gv, err := store.LoadSnapshot(0)
		require.NoError(t, err)

		tree, err := gv.GetTree("root")
		require.NoError(t, err)
		require.Equal(t, i, tree.snapshot_version)

		key := []byte(fmt.Sprintf("%d", i+1))
		value := []byte(fmt.Sprintf("%d", i+1))
		require.NoError(t, tree.Put(key, value))

		_, err = Commit(tree)
		require.NoError(t, err)
	}

	//fmt.Printf("error %s\n", store.loadsnapshottablestoram())

	store.versionrootfile.memoryfile[(loop_count-1)*8+7] = 1 // corrupt last entry
	store.versionrootfile.memoryfile[(loop_count-2)*8+7] = 1 // corrupt second last entry

	_, err = store.LoadSnapshot(0) // trigger recent version corruption
	require.Error(t, err)
	_, err = store.LoadSnapshot(4) // trigger second last version corruption
	require.Error(t, err)
	/*
		store.version_data[24] = 2
		store.version_data[24+16] = 1
		_, err = store.LoadSnapshot(1) // trigger recent version corruption
		require.Error(t, err)

		_, _, err = store.write([]byte{3, 0, 0, 0}) // write empty inner record
		_, err = store.LoadSnapshot(1)              // trigger recent version corruption
		require.Error(t, err)
	*/

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

}
