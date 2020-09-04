package main

import "flag"
import "os"
import "log"
import "math"
import "encoding/binary"
import "crypto/rc4"
import "path/filepath"
import "bytes"
import "runtime/pprof"
import "github.com/deroproject/graviton"

var rt = filepath.Join

const keysize uint64 = 64    // in bytes
const valuesize uint64 = 512 // in bytes
var stepsize = flag.Uint64("stepsize", 10, "Every commit will include this much data in MB")
var totalsize = flag.Uint64("totalsize", 500, "Total this much data will be written in MB ( use 0 for infinite )")
var db_directory = flag.String("db_directory", "/tmp", "DB will be created in this path, will be cleared on exit")
var memory = flag.Bool("memory", true, "DB will by default use memory backend (use -memory=false for disk based tests)")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")


var step uint64
var keys_written uint64

func main() {
	log.Printf("Graviton DB stress tester")
	log.Printf("NOTE: Do not use rotational media")

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *stepsize < 1 {
		*stepsize = 1
	}

	if *totalsize == 0 {
		*totalsize = math.MaxUint64
	}
	if *stepsize > 512 {
		*stepsize = 512
	}
	if *stepsize > *totalsize {
		*stepsize = *totalsize
	}

	log.Printf("Total Size (to be written): %d MB", *totalsize)
	log.Printf("Commit size: %d MB", *stepsize)

	var store *graviton.Store
	var err error
	if *memory {
		store, err = graviton.NewMemStore() // create a new  DB in RAM
		log.Printf("Using memory backend\n")
	} else {
		store, err = graviton.NewDiskStore(filepath.Join(*db_directory, "graviton_stress_db")) // create a new testdb in "/tmp/testdb"
		log.Printf("Using disk backend, db_directory: %s\n", filepath.Join(*db_directory, "graviton_stress_db"))
	}

	if err != nil {
		log.Fatalf("stress db creation err %s", err)
	}

	gv, err := store.LoadSnapshot(0)
	if err != nil {
		log.Fatalf("stress db  LoadSnapshot err %s", err)
	}

	write_tree, err := gv.GetTree("stress_testing")
	if err != nil {
		log.Fatalf("stress db  GetTree err %s", err)
	}

	for i := uint64(0); i < *totalsize / *stepsize; i++ {
		log.Printf("Running step %d    %f completed total keys %d", i, float64(i*100)/float64(*totalsize / *stepsize), keys_written)
		RunStep(store,write_tree)
	}
	log.Printf("Completed step %d    %f completed total keys %d", (*totalsize / *stepsize), float32(100), keys_written)

}

// each step consists of generating pseudorandom data, which is first committed and then verified after each step
func RunStep(store *graviton.Store, write_tree *graviton.Tree) {
	var read_tree *graviton.Tree
	values_count := (*stepsize * 1024 * 1024 / valuesize) + 1

	key_buf := make([]byte, values_count*keysize, values_count*keysize)
	value_buf := make([]byte, values_count*valuesize, values_count*valuesize)

	var cryptokey, cryptovalue [9]byte

	cryptokey[0] = 1
	binary.LittleEndian.PutUint64(cryptokey[1:], step)
	binary.LittleEndian.PutUint64(cryptovalue[1:], step)

	keycipher, _ := rc4.NewCipher(cryptokey[:])
	valuecipher, _ := rc4.NewCipher(cryptovalue[:])

	keycipher.XORKeyStream(key_buf[:], key_buf[:])
	valuecipher.XORKeyStream(value_buf[:], value_buf[:])

	

	for i := uint64(0); i < values_count; i++ {
		write_tree.Put(key_buf[i*keysize:(i+1)*keysize], value_buf[i*valuesize:(i+1)*valuesize])
		keys_written++
	}
	write_tree.Commit() //

	read_tree = write_tree // use same tree to read write
/*
	// now we will load another tree from storage without cache and read everything back and verify
	gv_read, err := store.LoadSnapshot(0)
	if err != nil {
		log.Fatalf("stress db  LoadSnapshot err %s", err)
	}

	read_tree, err := gv_read.GetTree("stress_testing")
	if err != nil {
		log.Fatalf("stress db  GetTree err %s", err)
	}
*/
	for i := uint64(0); i < values_count; i++ {
		if value, err := read_tree.Get(key_buf[i*keysize : (i+1)*keysize]); err == nil {

			if bytes.Compare(value, value_buf[i*valuesize:(i+1)*valuesize]) == 0 {
				// value macthed nothing to do

			} else { // value error
				log.Fatalf("value mismatched")
			}

		} else { // key not existent or other err, stop testing
			log.Fatalf("err occured while verifying tree err %s", err)
		}
	}
}
