// Copyright dero developers

/*
GravitonDB in short is  "ZFS for key-value stores".

GravitonDB is a pure Go key/value store having features unmatched by other software (such as Boltdb, berkeleydb, mysql, postgresql etc). The goal of the project is to provide a simple, fast, reliable, versioned, authenticated database for projects which require such features.

Since GravitonDB is meant to be used as such a low-level piece of functionality, simplicity is key. The API will be small and only focus on getting values and setting values. That's it.

	GravitonDB is a key value store having
		1) cryptographically authenticated ( a single hash verifies complete data store)
		2) append only
		3) versioning
		4) Writable snapshots
		5) exabyte storage capable, (tested upto Terabytes, if you can help with testing petabyte capability please discuss with authors)
		6) cryptographic proof capability to prove existence/non-existence of any arbitrary key
		7) ability to diff 2 trees in linear time
		8) designed and developed for DERO blockchain


	Features

		* Authenticated data store ( all keys, values are backed by blake 256 bit checksum)
		* Append only data store
		* Support of 2^64 trees ( Theoretically ) within a single data store. trees can be named and thus used as buckets
		* Versioning support ( all committed changes are versioned with ability to visit them at any point in time )
		* Snapshots ( multi tree commits  in a single version causing multi tree sync, each snapshot can be visited, appended and further modified, keys deleted, values modified etc, new keys, values stored )
		* Ability to iterate over all key-value pairs in a tree
		* Ability to diff between 2 trees in linear time and report all changes (insertions,deletions,modifications)
		* Minimal, small, simplified API
		* Theoretically support Exabyte data store, multi Terabyte tested internally
		* Decoupled storage layer, allowing use of object stores such as Ceph, AWS etc
		* Ability to generate cryptographic proofs which can prove key existance or non-existance ( proofs are around 1 KB )
		* Superfast proof generation time of around 1000 proofs per second per core
		* Support for disk based filesystem based persistant stores.
		* Support for memory based non-persistant stores
		* 100% code coverage
		* this is alpha software, we are still evaluating  a number of ideas


Eg. Minimal code, to write and read back a value (error checking is skipped)
	store, _ := NewDiskStore("/tmp/testdb")   // create a new testdb in "/tmp/testdb"
	ss, _ := store.LoadSnapshot(0)            // load most recent snapshot
	tree, _ := ss.GetTree("root")             // use or create tree named "root"
	tree.Put([]byte("key"), []byte("value"))  // insert a value
	Commit(tree)                             // commit the tree
	value, _ = tree.Get([]byte("key"))



Eg, Snapshots, see github.com/deroproject/gravitondb/examples/snapshot_example//snapshot_example.go



The design enables infinite trees with infinite snapshots. The design enables designs such as dedeuplicating backups, blockchains which
enable proving their data ( both state and content) to users etc

*/
package graviton
