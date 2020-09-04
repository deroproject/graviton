/*
Graviton snapshot example.
*/

package main

import "fmt"
import "github.com/deroproject/graviton"

func main() {

	key := []byte("key1")
	store, _ := graviton.NewDiskStore("/tmp/testdb")   // create a new testdb in "/tmp/testdb"
	//store, _ := graviton.NewMemStore()          // create a new  DB in RAM
	ss, _ := store.LoadSnapshot(0)         // load most recent snapshot
	tree, _ := ss.GetTree("root")          // use or create tree named "root"
	tree.Put(key, []byte("commit_value1")) // insert a value
	commit1, _ := graviton.Commit(tree)         // commit the tree
	tree.Put(key, []byte("commit_value2")) // overwrite existing value
	commit2, _ := graviton.Commit(tree)         // commit the tree again

	// at this point, you have done 2 commits
	// at first commit,  "root" tree contains  "key : commit_value1"
	// at second commit,  "root" tree contains  "key : commit_value2"

	// we will traverse now commit1 snapshot
	ss, _ = store.LoadSnapshot(commit1)
	tree, _ = ss.GetTree("root")
	value, err := tree.Get(key)
	fmt.Printf(" snapshot%d  key %s value %s err %s\n", ss.GetVersion(), string(key), string(value), err)

	// we will traverse now commit2 snapshot
	ss, _ = store.LoadSnapshot(commit2)
	tree, _ = ss.GetTree("root")
	value, err = tree.Get(key)
	fmt.Printf(" snapshot%d  key %s value %s err %s\n", ss.GetVersion(), string(key), string(value), err)
}
