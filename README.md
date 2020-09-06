
# Graviton Database: ZFS for key-value stores.

Graviton Database is simple, fast, versioned, authenticated, embeddable key-value store database in pure GOLANG.  
Graviton Database in short is like "ZFS for key-value stores" in which every write is tracked, versioned and authenticated with cryptograhic proofs. Additionally it is possible to take snapshots of database. Also it is possible to use simple copy,rsync commands for database backup even during live updates without any possibilities of database corruption.

![Graviton: ZFS for key-value stores](images/GRAVITON.png?raw=true "Graviton: ZFS for key-value stores")

## Project Status
Graviton is currently alpha software. Almost full unit test coverage and randomized black box testing are used to ensure database consistency and thread safety. The project already has 100% code coverage. A number of decisions such as change,rename APIs, handling errors, hashing algorithms etc. are being evaluated and open for improvements and suggestions.

## Features
Graviton Database in short is  "ZFS for key-value stores".

* Authenticated data store (All keys, values are backed by blake 256 bit checksum).
* Append only data store.
* Support of 2^64 trees (Theoretically) within a single data store. Trees can be named and thus used as buckets.
* Support of values version tracking. All committed changes are versioned with ability to visit them at any point in time. 
* Snapshots (Multi tree commits in a single version causing multi bucket sync, each snapshot can be visited, appended and further modified, keys deleted, values modified etc., new keys, values stored.)
* Ability to iterate over all key-value pairs in a tree.
* Ability to diff between 2 trees in linear time and report all changes of Insertions, Deletions, Modifications.)
* Minimal and simplified API.
* Theoretically support Exabyte data store, Multi TeraByte tested internally.
* Decoupled storage layer, allowing use of object stores such as Ceph, AWS etc.
* Ability to generate cryptographic proofs which can prove key existance or non-existance (Cryptographic Proofs are around 1 KB.)
* Superfast proof generation time of around 1000 proofs per second per core.
* Support for disk based filesystem based persistant stores.
* Support for memory based non-persistant stores.
* 100% code coverage



## Table of Contents
1. [Getting Started](#getting-started) 
1. [Installing](#installing) 
1. [Opening and Using the Database](#opening-and-using-the-database) 
1. [Graviton Tree](#graviton-tree) 
1. [Using key,value pairs](#using-keyvalue-pairs) 
1. [Iterating over keys](#iterating-over-keys) 
1. [Snapshots](#snapshots) 
1. [Diffing](#diffing) (Diffing of 2 trees to detect changes between versions or compare 2 arbitrary trees in linear time.)
1. [GravitonDB Backups](#gravitondb-backups) 
1. [Stress testing](#stress-testing) 
1. [Graviton Internals](#graviton-internals) 
1. [Lines of Code](#lines-of-Code) 
1. [TODO](#todo) 
1. [Comparison with other databases](#comparison-with-other-databases) (Mysql, Postgres, LevelDB, RocksDB, LMDB, Bolt etc.)
1. [License](#license) 


GNU General Public License v3.0

## Getting Started
### Installing
To start using Graviton DB, install Go and run go get:

```go get github.com/deroproject/graviton/...```

This will retrieve the library and build the library


### Opening and Using the Database

The top-level object in Graviton is a Store. It is represented as a directory with multiple files on server's disk and represents a consistent snapshot of your data at all times.

Example code to open database:

    package main

    import "fmt"
    import "github.com/deroproject/graviton"

    func main() {
	   //store, _ := graviton.NewDiskStore("/tmp/testdb")   // create a new testdb in "/tmp/testdb"
        store, _ := graviton.NewMemStore()            // create a new  DB in RAM
        ss, _ := store.LoadSnapshot(0)           // load most recent snapshot
        tree, _ := ss.GetTree("root")            // use or create tree named "root"
        tree.Put([]byte("key"), []byte("value")) // insert a value
        graviton.Commit(tree)                  // commit the tree
        value, _ := tree.Get([]byte("key"))
        fmt.Printf("value retrived from DB \"%s\"\n", string(value))
    }

    //NOTE: Linux (or other platforms) have open file limit for 1024. 
    //    Default limits allows upto 2TB of Graviton databases.

### Graviton Tree
A Tree in Graviton DB acts like a bucket in BoltDB or a ZFS dataset. It is named and can contain upto 128 byte names. Any store can contain infinite trees. Each tree can also contain infinite key-value pairs. However, practically being limited by the server or system storage space.

Each tree can be accessed with its merkle root hash using "*GetTreeWithRootHash*" API. Also each tree maintains its own separate version number and any specific version can be used *GetTreeWithVersion*. Note that each tree can also have arbitrary tags and any tagged tree can be accessed using the tag *GetTreeWithTag*. Also, 2 arbitrary trees can diffed in linear time and relevant changes detected.

    NOTE: Tree tags or names cannot start with ':' .

### Using key,value pairs

To save a key/value pair to a tree ( or bucket), use the `tree.Put()` function:

```go
        tree, _ := ss.GetTree("root") 
        tree.Put([]byte("answer"), []byte("44")) // insert a value
        graviton.Commit(tree)  // make the tree persistant by storing it in backend disk
```

This will set the value of the `"answer"` key to `"44"` in the `root`
tree. To retrieve this value, we can use the `tree.Get()` function:

```go
	tree, _ := ss.GetTree("root") 
	v,_ := tree.Get([]byte("answer"))
	fmt.Printf("The answer is: %s\n", v)
```

The `Get()` function returns an error because its operation is guaranteed to work (unless there is some kind of system failure which we try to report). If the key exists then it will return its byte slice value. If it doesn't exist then it
will return  an error. 

### Iterating over keys

Graviton stores its keys in hash byte-sorted order within a tree. This makes sequential
iteration over these keys extremely fast. To iterate over keys GravitonDB uses a
`Cursor`:

```go
	// Assume "root" tree exists and has keys
    tree, _ := store.GetTree("root") 
	c := tree.Cursor()

	for k, v, err := c.First(); err == nil; k, v, err = c.Next() { 
		fmt.Printf("key=%s, value=%s\n", k, v)
	}
```

The cursor allows you to move to a specific point in the list of keys and move
forward or backward through the keys one at a time.

The following functions are available on the cursor:

```
First()  Move to the first key.
Last()   Move to the last key.
Next()   Move to the next key.
Prev()   Move to the previous key.
```

Each of those functions has a return signature of `(key []byte, value []byte, err error)`.
When you have iterated to the end of the cursor then `Next()` will return an error `ErrNoMoreKeys`.  You must seek to a position using `First()`, `Last()`
before calling `Next()` or `Prev()`. If you do not seek to a position then these functions will return an error.


### Snapshots
Snapshot refers to collective state of all buckets + data + history. Each commit( tree.Commit() or Commit(tree1, tree2 .....)) creates a new snapshot in the store.Each snapshot is represented by an incremental uint64 number, 0 represents most recent snapshot.
Snapshots can be used to access any arbitrary state of entire database at any point in time.

Example code for snapshots:

    package main

    import "fmt"
    import "github.com/deroproject/graviton"

    func main() {
    	   key := []byte("key1")
	   //store, _ := graviton.NewDiskStore("/tmp/testdb")   // create a new testdb in "/tmp/testdb"
	   store, _ := graviton.NewMemStore()          // create a new  DB in RAM
	   ss, _ := store.LoadSnapshot(0)         // load most recent snapshot
	   tree, _ := ss.GetTree("root")          // use or create tree named "root"
	   tree.Put(key, []byte("commit_value1")) // insert a value
	   commit1, _ := graviton.Commit(tree)         // commit the tree
	   tree.Put(key, []byte("commit_value2")) // overwrite existing value
	   commit2, _ := graviton.Commit(tree)         // commit the tree again

	   // at this point, you have done 2 commits
	   // at first commit or snapshot,  "root" tree contains  "key1 : commit_value1"
	   // at second commit or snapshot,  "root" tree contains  "key1 : commit_value2"

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

### Diffing
#### Diffing of 2 trees to detect changes between versions or compare 2 arbitrary trees in linear time.
Two arbitrary trees can be diffed in linear time to detect changes. Changes are of 3 types insertions, deletions and modifications (Same key but value changed). If the reported changes are applied to base tree, it will be equivalent to the head tree being compared.

    func Diff(base_tree, head_tree *Tree, deleted, modified, inserted DiffHandler) (err error)

Diffhandler is a callback function of the following type having k,v as arguments

    type DiffHandler func(k, v []byte)

The algorithm is linear time in the number of changes. Eg. a tree with billion KVs can be diffed with parent almost instantaneously.



### GravitonDB Backups
Use simple commands like cp, copy or rsync to sync a Graviton database even while the database is being updated. However, as the database might be continuously appending, backup will always lag a bit. And note that the database or backups will NEVER get corrupted during copying while commits are being done.

### Stress Testing
A mini tool to do single thread testing is provided which can be used to perform various tests on memory or disk backend.

    go run github.com/deroproject/graviton/cmd/stress

See help using `--help` argument. To use disk backend, use `--memory=false`


### Graviton Internals
Internally, all trees are stored within a base-2 merkle with collapsing path. This means if tree has 4 billion key-value pairs, it will only be 32 level deep.This leads to tremendous savings in storage space.This also means when you modify an existing key-value, only limited amount of nodes are touched.


### Lines of Code
    ~/tools/gocloc   --by-file  node_inner.go tree.go snapshot.go proof.go node_leaf.go  store.go node.go  hash.go  const.go doc.go  diff_tree.go cursor.go 
    -----------------------------------------------------------------
    File           files          blank        comment           code
    -----------------------------------------------------------------
    node_inner.go                    76             33            364
    store.go                         69             22            250
    tree.go                          75             71            250
    proof.go                         30             16            171
    snapshot.go                      36             18            155
    node_leaf.go                     29              3            150
    diff_tree.go                     34             33            133
    cursor.go                        21             15            106
    node.go                           5              3             35
    const.go                          4              0             21
    hash.go                           7              2             19
    doc.go                           16             42              1
    -----------------------------------------------------------------
    TOTAL             12            402            258           1655
    -----------------------------------------------------------------

## TODO 
* Currently it is not optimized for speed and GC (Garbage collection).
* Expose/build metrics.
* Currently, we have error reportingapi to reports rot bits, but nothing about disks corruption, should we discard such error design and make the API simpler (except snapshots, tree loading, commiting, no more errors ). More discussion required on this hard-disk failures,errors etc. required.


### Comparison with other databases
None of the following databases provides ability to traverse back-in-time for each and every commit. GravitonDB is the only DB which provides back-in-time. Also presently GravitonDB is the only database which can diff between 2 trees in linear time. Let's compare between other features of some databases.

#### Postgres, MySQL, & other relational databases

Relational databases structure data into rows and are only accessible through
the use of SQL. This approach provides flexibility in how you store and query
your data but also incurs overhead in parsing and planning SQL statements. GravitonDB
accesses all data by a byte slice key. This makes GravitonDB fast to read and write
data by key but provides no built-in support for joining values together.

Most relational databases (with the exception of SQLite) are standalone servers
that run separately from the application. This gives systems
flexibility to connect multiple application servers to a single database
server but also adds overhead in serializing and transporting data over the
network. Graviton runs as a library included in your application so all data access
has to go through your application's process. This brings data closer to your
application but limits multi-process access to the data.



#### LevelDB, RocksDB

LevelDB and its derivatives (RocksDB, HyperLevelDB) are similar to Graviton in that
they are libraries bundled into the application, However, their underlying
structure is a log-structured merge-tree (LSM tree). An LSM tree optimizes
random writes by using a write ahead log and multi-tiered, sorted files called
SSTables. Graviton uses a base 2 merkle tree internally. Both approaches
have trade-offs.

If you require a high random write throughput or you need to use
spinning disks then LevelDB could be a good choice unless there are requirements of versioning, authenticated proofs or other features of Graviton database.

#### LMDB, BoltDB

LMDB, Bolt are architecturally similar. Both use a B+ tree, have ACID semantics with fully serializable transactions, and support lock-free MVCC using a single writer and multiple readers.

In-addition LMDB heavily focuses on raw performance while BoltDB focus on simplicity and ease of use. For example, LMDB allows several unsafe actions such as direct writes for the sake of performance. Bolt opts to disallow actions which can leave the database in a corrupted state. The only exception to this in Bolt is `DB.NoSync`.GravitonDB does not leave the database in corrupted state at any point in time.


In-addition LMDB, BoltDB doesn't support versioning, snapshots, linear diffing etc. features only Graviton provides such features for now.


### License 
[GNU General Public License v3.0](https://github.com/deroproject/graviton/blob/master/LICENSE)
