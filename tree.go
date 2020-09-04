package graviton

import "fmt"
import "bytes"

import "encoding/binary"

import "golang.org/x/xerrors"

// after commits all leaves will be discarded from ram,
// all inner nodes will be discarded above this level
// todo this parameter must be tunable by the user or by system automagically
const innernode_cache_level = 17

// Tree structure which is the end result,
// TODO: tree does not cache anything currently, just caching the top level tree entries, will increase the speed by X multiplication factors.
type Tree struct {
	store    *Store
	root     *inner // main root , this provides all proof checking, authentication, snapshot etc
	treename string // tree name
	size     int
	Tags     []string // tags used while commit, will get cleaned after commit

	tmp_buffer bytes.Buffer
}

// Get current version number of tree
func (t *Tree) GetVersion() uint64 {
	return t.root.version_current
}

// Get parent version number of tree from which this tree was derived, they might not be sequential but they will be monotonically increasing
// this can be used to build out a DAG
func (t *Tree) GetParentVersion() uint64 {
	return t.root.version_previous
}

// put a key value in the tree, if the value exists, it's overwritten.
// ToDO: it should ignore duplicate key value, if first using a get and then a put
//
func (t *Tree) Put(key, value []byte) error {
	return t.putRaw(sum(key), key, value)
}
func (t *Tree) putRaw(keyhash [HASHSIZE]byte, key, value []byte) error {
	if len(value) > MAX_VALUE_SIZE {
		return xerrors.Errorf("value is longer then max allowed value size, %d > %d", len(value), MAX_VALUE_SIZE)
	}

	leaf := newLeaf(keyhash, key, value)
	return t.root.Insert(t.store, leaf)
}

// Get a specifically value associated with a key
// TODO, we need to expose this in other forms so as memory allocations and better error detection could be done
func (t *Tree) Get(key []byte) ([]byte, error) {
	return t.getRaw(sum(key))
}

// Get a specific value associated with a specific key hash
// TODO, this api should not be exposed
func (t *Tree) getRaw(keyhash [HASHSIZE]byte) ([]byte, error) {
	return t.root.Get(t.store, keyhash)
}

// Give the merkle hash of the entire tree
func (t *Tree) Hash() (h [HASHSIZE]byte, err error) {

	hash, err := t.root.Hash(t.store)
	if err != nil {
		return
	}
	copy(h[:], hash)
	return
}

// delete a specific key from the tree
func (t *Tree) Delete(key []byte) error {
	_, _, err := t.root.Delete(t.store, sum(key))
	return err
}

// Check whether the tree is currently dirty or not
func (t *Tree) IsDirty() bool {
	return t.root.isDirty()
}

// Generate proof of any key, which can be used to prove whether the key exists or not.Please note that
// the tree root hash (tree.Hash()) is not part of the structure and must be available to the verifier separately
// for eg. in an encrypted blockchain, the entire state is carried forward from block to block, this state can be
// queried from a number of sources and then it is verified
func (t *Tree) GenerateProof(key []byte) (*Proof, error) {
	var p Proof
	err := t.generateProofRaw(sum(key), &p)
	return &p, err
}

func (t *Tree) generateProofRaw(key [HASHSIZE]byte, proof *Proof) error {
	return t.root.Prove(t.store, key, proof)
}

// Commit the tree (or a number of trees) to persistance, write a new snapshot which can be accessed henceforth without any modifications
// Commiting multiple trees or multiple changes as batch is much more effecient than
// committing each change independently.
func Commit(trees ...*Tree) (committed_version uint64, err error) {
	if len(trees) == 0 {
		return 0, nil
	}

	trees[0].store.commitsync.Lock()
	defer trees[0].store.commitsync.Unlock()

	gv, err := trees[0].store.LoadSnapshot(0)
	if err != nil {
		return
	}

	for _, tree := range trees {
		if err = gv.commit(tree); err != nil {
			return
		}
	}

	// version should be committed only if it has changed
	// this block of code writes reverse version pointers
	_, highest_version, findex, fpos := trees[0].store.findhighestsnapshotinram()
	var valuearray [HASHSIZE]byte
	var key [512]byte
	key[0] = ':'
	key[1] = ':'
	done := 2
	done += binary.PutUvarint(key[done:], highest_version)

	valuesize := encode(findex, fpos, valuearray[:]) //store link of previous version root to previous version findex,fpos

	if err = gv.vroot.Insert(gv.store, newLeaf(sum([]byte(key[:done])), []byte(key[:done]), valuearray[:valuesize])); err == nil {
		if findex, fpos, err = trees[0].commit_inner(gv, true, 0, gv.vroot); err == nil { // we must discard any version changes
			if err = trees[0].store.writeVersionData(gv.vroot.version_current, findex, fpos); err == nil {
				committed_version = gv.vroot.version_current
			}
		}
	}

	//fmt.Printf("committing version tree %x\n", gv.vroot.Hash(gv.store))

	// this is we are cleaning up the trees, should we report bak any error , why  should this code be here
	/*
		for _, tree := range trees {
			newtree, err1 := gv.GetTreeWithVersion(tree.bucket, tree.GetVersion()) // get last committed version of the current branch
			if err1 == nil {
				*tree = *newtree
			} else {
				return 0, err1
			}
		}
	*/
	return
}

// commit a single tree at a time
func (gv *Snapshot) commit(tree *Tree) (err error) {
	var findex, fpos uint32

	tree.size = 0
	if tree.IsDirty() {
		if findex, fpos, err = tree.commit_inner(gv, false, 0, tree.root); err != nil {
			return err
		}
	} else {
		findex, fpos = tree.root.Position()
	}

	var roothash [HASHSIZE]byte
	if roothash, err = tree.Hash(); err == nil {

		// at this point tree cannot be dirty

		//root_size := tree.size

		var valuearray [HASHSIZE]byte
		valuesize := encode(findex, fpos, valuearray[:])
		valuebuf := valuearray[:valuesize]

		var key [512]byte
		key[0] = ':'
		done := 1
		done += copy(key[done:], []byte(tree.treename))
		done += binary.PutUvarint(key[done:], tree.root.version_current)

		if err = gv.vroot.Insert(tree.store, newLeaf(sum(key[:done]), key[:done], valuebuf[:])); err == nil { // always ensure tree is accessible by its bucket & version number

			done = 1
			done += copy(key[done:], []byte(tree.treename))
			done += copy(key[done:], roothash[:])
			if err = gv.vroot.Insert(tree.store, newLeaf(sum(key[:done]), key[:done], valuebuf[:])); err == nil {

				if err = gv.vroot.Insert(tree.store, newLeaf(sum(roothash[:]), roothash[:], valuebuf[:])); err == nil {

					for i := 0; i < len(tree.Tags) && err == nil; i++ {
						err = gv.vroot.Insert(tree.store, newLeaf(sum([]byte(tree.Tags[i])), []byte(tree.Tags[i]), valuebuf[:]))
					}
				}
			}
		}
	}

	tree.tmp_buffer = bytes.Buffer{} // so as storage space could be be reclaimed

	//version_size := tree.size
	//_ = version_size

	//_ = root_size
	// we should expose these stats somehow so as users could make some judgements about overheads
	//fmt.Printf("tree committed findex %d fpos %d err %s  committed bytes  rootsize:%d versionsize:%d  version: %d pversion %d\n", findex, fpos, err, root_size, version_size, tree.GetVersion(), tree.GetParentVersion())

	return

}

// commit the tree to disk, the current version
func (t *Tree) Commit(tags ...string) error {
	t.Tags = tags
	_, err := Commit(t)
	return err

}

// Reload the tree from the disk, causing all current changes to be discarded,
func (t *Tree) Discard() error {
	gv, err := t.store.LoadSnapshot(0)
	if err == nil {
		var newtree *Tree
		if newtree, err = gv.GetTreeWithVersion(t.treename, t.GetVersion()); err == nil { // get last committed version of the current branch
			*t = *newtree
		}
	}

	return err // it will generally be nil
}

// this is never recursive
// leaf marshalling is done at only one place while  committing
// this is done here avoid an allocation  which can be done from the stack
func (t *Tree) commit_leaf(level int, l *leaf) (findex uint32, fpos uint32, err error) {

	t.tmp_buffer.Reset()
	var tbuf [10]byte

	size := binary.PutUvarint(tbuf[:], uint64(len(l.key)))
	t.tmp_buffer.Write(tbuf[:size])
	if len(l.key) > 0 {
		t.tmp_buffer.Write(l.key[:])
	}
	size = binary.PutUvarint(tbuf[:], uint64(len(l.value)))
	t.tmp_buffer.Write(tbuf[:size])
	if len(l.value) > 0 {
		t.tmp_buffer.Write(l.value[:])
	}

	// here we must write it to store
	t.size += len(t.tmp_buffer.Bytes())
	findex, fpos, err = t.store.write(t.tmp_buffer.Bytes())
	l.findex = findex
	l.fpos = fpos
	l.dirty = false

	l.loaded_partial = true
	l.key = nil
	l.value = nil
	return

}

// this is mostly recursive and must skip non modified branches reusing them
// and must skip dirty parts
func (t *Tree) commit_inner(gv *Snapshot, specialversion bool, level int, in *inner) (findex uint32, fpos uint32, err error) {

	var old_old_version, old_version uint64
	var success bool

	if in.left == nil { // handle all left cases
		in.left_findex, in.left_fpos = 0, 0
	} else if !in.left.isDirty() {
		in.left_findex, in.left_fpos = in.left.Position()
	} else { // node is dirty and must be written

		switch v := in.left.(type) { // commit left  branch
		case *inner:
			in.left_findex, in.left_fpos, err = t.commit_inner(gv, specialversion, level+1, v)
		case *leaf:
			in.left_findex, in.left_fpos, err = t.commit_leaf(level+1, v)
		default:
			err = fmt.Errorf("unknown node type")
		}
	}
	if err != nil {
		return
	}

	if in.right == nil { // handle all rights cases
		in.right_findex, in.right_fpos = 0, 0
	} else if !in.right.isDirty() {
		in.right_findex, in.right_fpos = in.right.Position()
	} else { // node is dirty and must be written

		switch v := in.right.(type) { // commit right  branch
		case *inner:
			in.right_findex, in.right_fpos, err = t.commit_inner(gv, specialversion, level+1, v)
		case *leaf:
			in.right_findex, in.right_fpos, err = t.commit_leaf(level+1, v)
		default:
			err = fmt.Errorf("unknown node type")
		}
	}

	if err != nil {
		return
	}

	// increment and reserve a version number, this takes 1 read iop and 1 write iop
	if in.bit == 0 {
		old_old_version = in.version_previous
		old_version = in.version_current

		if specialversion { // this is for the version root
			// lets increment the version number and put it again
			_, in.version_current, _, _ = t.store.findhighestsnapshotinram() // setup index properly
			in.version_current++
			in.version_previous = old_version

		} else {

			in.version_previous = old_version
			// now gets highest version for this bucket from version root, increment and store it again
			// Todo below 2 operations must be integrated or protected with a lock to avoid race condition

			var highest_version uint64
			if highest_version, err = gv.GetTreeHighestVersion(t.treename); err == nil {

				// lets increment the highest version
				highest_version++

				if err = gv.putTreeHighestVersion(t.treename, highest_version); err == nil {

					in.version_current = highest_version
				}

			}
			if err != nil {
				return 0, 0, err
			}

		}

		//fmt.Printf("oldold %d old %d current %d\n", old_old_version, old_version, in.version_current)
		// below block has been replaced
		///	defer func() {
		//		if !success { // if this ever occurs, we will  skip a version number
		//			in.version_current = old_version
		//			in.version_previous = old_old_version
		//		}
		///	}()

	}

	var buf [128]byte
	var done int
	if done, err = in.MarshalTo(t.store, buf[:], t.treename); err == nil {

		// here we must write it to store
		t.size += len(buf)
		findex, fpos, err = t.store.write(buf[:done])
		if err == nil {
			in.findex = findex
			in.fpos = fpos
			in.dirty = false
			success = true
		}

		if success && in.bit >= innernode_cache_level {
			in.left, in.right = nil, nil
			in.loaded_partial = true
		}

		if in.bit == 0 && !success { // if this ever occurs, we will  skip a version number
			in.version_current = old_version
			in.version_previous = old_old_version
		}
	}
	return
}

// encode findex,fpos
func encode(findex, fpos uint32, buf []byte) int {
	bytes_written := binary.PutUvarint(buf[:], uint64(findex))
	done := bytes_written
	bytes_written = binary.PutUvarint(buf[done:], uint64(fpos))
	return bytes_written + done
}

// decode findex,fpos
func decode(buf []byte) (uint32, uint32) {
	var size int
	var findex, fpos uint64
	findex, size = binary.Uvarint(buf[:])
	fpos, size = binary.Uvarint(buf[size:])
	return uint32(findex), uint32(fpos)
}
