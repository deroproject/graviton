package graviton

import "fmt"
import "encoding/binary"

// 		Snapshot are used to access any arbitrary snapshot of entire database at any point in time
// 		snapshot refers to collective state of all trees + data (key-values) + history
// 		each commit ( tree.Commit() or Commit(tree1, tree2 .....)) creates a new snapshot
// 		each snapshot is  represented by an incrementing uint64 number, 0 represents most recent snapshot.
// 	TODO we may need to provide API to export DB at specific snapshot version
type Snapshot struct {
	store        *Store
	version      uint64
	findex, fpos uint32
	vroot        *inner
}

// Load a specific snapshot from the store,  0th  version = load most recent version as a special case
// note: 0th tree is not stored in disk
// also note that commits are being done so versions might be change
func (store *Store) LoadSnapshot(version uint64) (*Snapshot, error) {
	if !store.version_data_loaded {
		if err := store.loadsnapshottablestoram(); err != nil {
			return nil, err
		}
	}

	_, highest_version, findex, fpos := store.findhighestsnapshotinram() // only latest version can be reached from the table
	if version > highest_version {
		return nil, fmt.Errorf("Database highest version: %d you requested %d.Not Possible!!", highest_version, version)
	}

	if version <= 0 || version == highest_version { // user requested most recent version
		if findex == 0 && fpos == 0 { // if storage is newly create, lets build up a new version root
			return &Snapshot{store: store, version: highest_version, findex: uint32(findex), fpos: uint32(fpos), vroot: newInner(0)}, nil
		} else {
			if _, vroot, err := store.loadrootusingpos(findex, fpos); err != nil {
				return nil, err
			} else {
				return &Snapshot{store: store, version: highest_version, findex: uint32(findex), fpos: uint32(fpos), vroot: vroot}, nil

			}
		}
	}
	// user requested an arbitrary version between 1 and highest_version -1
	_, hvroot, err := store.loadrootusingpos(findex, fpos) // load highest version root tree
	if err != nil {
		return nil, err
	}

	var key = [512]byte{':', ':'} // now use it to locate specific version tree
	done := 2
	done += binary.PutUvarint(key[done:], version)

	eposition, err := hvroot.Get(store, sum(key[:done]))
	if err != nil {
		return nil, err
	}

	findex, fpos = decode(eposition)
	_, vroot, err := store.loadrootusingpos(findex, fpos)
	if err != nil {
		return nil, err
	}
	return &Snapshot{store: store, version: version, findex: findex, fpos: fpos, vroot: vroot}, nil
}

func (store *Store) loadrootusingpos(findex, fpos uint32) (string, *inner, error) {
	var buf [512]byte

	bytes_count, err := store.read(findex, fpos, buf[:])
	if bytes_count >= 3 {
		tmp := &inner{hash: make([]byte, 0, HASHSIZE)}
		err := tmp.Unmarshal(buf[:bytes_count])
		if err != nil {
			return "", nil, err
		} else {
			tmp.findex, tmp.fpos = findex, fpos
			return string(tmp.bucket_name), tmp, nil
		}
	}
	return "", nil, err
}

// load tree using the specfic global version
func (s *Snapshot) loadTree(key []byte) (tree *Tree, err error) {

	var bname string
	var root *inner
	var position []byte
	if position, err = s.vroot.Get(s.store, sum(key)); err == nil { // underscore is first character

		if bname, root, err = s.store.loadrootusingpos(decode(position)); err == nil {
			tree = &Tree{store: s.store, root: root, treename: bname}
			tree.Hash()
		}
	}

	return tree, err
}

func (store *Store) findhighestsnapshotinram() (index int, version uint64, findex, fpos uint32) {
	var highest_version uint64
	for i := 0; i < internal_MAX_VERSIONS_TO_KEEP; i++ {
		if highest_version < binary.LittleEndian.Uint64(store.version_data[i*internal_VERSION_RECORD_SIZE:]) {
			index = i
			version = binary.LittleEndian.Uint64(store.version_data[i*internal_VERSION_RECORD_SIZE:])
			findex = uint32(binary.LittleEndian.Uint64(store.version_data[i*internal_VERSION_RECORD_SIZE+8:]))
			fpos = uint32(binary.LittleEndian.Uint64(store.version_data[i*internal_VERSION_RECORD_SIZE+16:]))
			highest_version = version
		}
	}
	return
}

// Load a versioned tree from the store all trees have there own version number
func (s *Snapshot) GetTreeWithVersion(treename string, version uint64) (*Tree, error) {
	var buf = [512]byte{':'}

	if err := check_tree_name(treename); err != nil {
		return nil, err
	}

	if version == 0 {
		return &Tree{root: newInner(0), treename: treename, store: s.store}, nil
	}

	done := 1
	done += copy(buf[done:], []byte(treename))
	done += binary.PutUvarint(buf[done:], version)
	return s.loadTree(buf[:done])
}

// Gets the snapshot version number
func (s *Snapshot) GetVersion() uint64 {
	return s.version
}

// Gets highest stored version number of the specific bucket
func (s *Snapshot) GetTreeHighestVersion(treename string) (uint64, error) {
	var buf = [512]byte{':'}

	if err := check_tree_name(treename); err != nil {
		return 0, err
	}

	done := 1
	done += copy(buf[done:], []byte(treename))

	vversion, err := s.vroot.Get(s.store, sum(buf[:done]))
	if err != nil { // return no found
		return 0, nil // fmt.Errorf("version is not stored")
	}

	version, versionsize := binary.Uvarint(vversion)
	if versionsize <= 0 {
		return 0, fmt.Errorf("version could not be decoded probably data corruption")
	}

	return version, nil
}

// Gets most recent tree committed to the store
func (s *Snapshot) GetTree(treename string) (*Tree, error) {
	if version, err := s.GetTreeHighestVersion(treename); err != nil {
		return nil, err
	} else {
		return s.GetTreeWithVersion(treename, version)
	}
}

// Gets the tree which has specific roothash
func (s *Snapshot) GetTreeWithRootHash(roothash []byte) (*Tree, error) {
	return s.loadTree(roothash)
}

// Gets the tree which has specific tag
// 		NOTE: same tags might point to different trees in different snapshots of db
func (s *Snapshot) GetTreeWithTag(tag string) (*Tree, error) {
	return s.loadTree([]byte(tag))
}

func check_tree_name(bucket string) error {
	if len(bucket) > TREE_NAME_LIMIT {
		return fmt.Errorf("Bucket name is too big than allowed limit of 127 bytes")
	}

	if len(bucket) >= 1 && bucket[0] == ':' {
		return fmt.Errorf("Bucket cannot start with ':'")
	}
	return nil
}

// store highest version of tree
func (s *Snapshot) putTreeHighestVersion(treename string, version uint64) error {
	var buf = [512]byte{':'}
	var value [12]byte

	if err := check_tree_name(treename); err != nil {
		return err
	}

	done := 1
	done += copy(buf[done:], []byte(treename))
	valuesize := binary.PutUvarint(value[:], version)

	leaf := newLeaf(sum(buf[:done]), buf[:done], value[:valuesize])
	return s.vroot.Insert(s.store, leaf)
}
