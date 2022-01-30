package main

import "fmt"
import "github.com/deroproject/graviton"

func main() {
	//store, _ := graviton.NewDiskStore("/tmp/testdb")   // create a new testdb in "/tmp/testdb"
	store, _ := graviton.NewMemStore()            // create a new  DB in RAM
	ss, _ := store.LoadSnapshot(0)           // load most recent snapshot
	tree, _ := ss.GetTree("root")            // use or create tree named "root"
	tree.Put([]byte("key"), []byte("value")) // insert a value
	_, _ = graviton.Commit(tree)                  // commit the tree
	value, _ := tree.Get([]byte("key"))

	fmt.Printf("value retrived from DB \"%s\"\n", string(value))

}
