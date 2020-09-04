/*
Graviton showcase data corruption example.
*/

package main

import "os"
import "fmt"
import "log"
import "bytes"
import "io/ioutil"
import "path/filepath"
import "github.com/deroproject/graviton"


func handle_error(err error, msg string){
	if err != nil {
		log.Fatalf("%s  err:\n",msg, err)
	}
}

func main() {



	key := []byte("key1key1key1key1key1key1key1key1key1key1key1key1key1key1")
	value :=           []byte("This value is good") // an value, which will be corrupted later on disk to showcase detection
	value_corrupted := []byte("Corrupted value !!")

	dir, err := ioutil.TempDir("", "graviton_showcase_")
	handle_error(err,"err opening database")
	
	defer os.RemoveAll(dir) // clean up

	store, err := graviton.NewDiskStore(dir)
	handle_error(err,"err opening diskstore")
	
	gv, err := store.LoadSnapshot(0)
	handle_error(err,"err opening snapshot")
	tree, err := gv.GetTree("treename")
	handle_error(err,"err opening tree")

	err = tree.Put(key, value); 
	handle_error(err,"err writing value to diskstore")

	 _, err = graviton.Commit(tree); 
	 handle_error(err,"err committing tree to diskstore")

    gv, err = store.LoadSnapshot(0)
	handle_error(err,"err opening snapshot")
	tree, err = gv.GetTree("treename")
	handle_error(err,"err opening tree")
	value,err = tree.Get(key)

	fmt.Printf("Reading value before corruption \"%s\" err %s\n", string(value), err)


	// now the key has been persisted and verified now we will corrupt the value
	file, err := os.OpenFile(filepath.Join(dir,"0","0","0","0.dfs"), os.O_RDWR, 0644)
	handle_error(err,"err opening db file in raw mode for corruption")

	file_contents,err := ioutil.ReadFile( filepath.Join(dir,"0","0","0","0.dfs"))
	handle_error(err,"err read db file in raw mode for corruption")

	index := bytes.Index(file_contents,value)
	if index < 0 {
		handle_error(err,"err could not find value in DB")
	}

	//fmt.Printf("contain count %d\n", bytes.Count(file_contents,value))

	file.WriteAt(value_corrupted, int64(index))
	file.Close()

	gv, err = store.LoadSnapshot(0)
	handle_error(err,"err opening snapshot")
	tree1, err := gv.GetTree("treename")
	handle_error(err,"err opening tree")
	value_back,err := tree1.Get(key)

	if err != nil {
		fmt.Printf("Value Corruption detected %s\n",err)
		return
	}else{

	fmt.Printf("Reading value after corruption \"%s\" err %s\n", string(value_back), err)
}


}
