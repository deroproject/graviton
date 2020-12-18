package graviton

import "io"

import "math"

import "encoding/binary"
import "golang.org/x/xerrors"

// TODO optimize these structures for less RAM/DISK
type inner struct {
	hash        []byte
	hash_backer [HASHSIZE]byte

	bucket_name []byte // only valid if bit is zero

	fpos, findex             uint32 // 0 values are invalid
	left_fpos, left_findex   uint32
	right_fpos, right_findex uint32

	left, right node

	version_previous uint64 // previous version
	version_current  uint64 // currentversion

	dirty, loaded_partial bool
	bit                   uint8
}

func newInner(bit uint8) *inner {
	in := &inner{
		bit:   bit,
		dirty: true, // new nodes are dirty by default
	}

	in.hash = in.hash_backer[:0]
	return in
}

func (in *inner) isDirty() bool {
	return in.dirty
}

func (in *inner) isEmpty() bool {
	return in.left == nil && in.right == nil
}

func (in *inner) lhash(store *Store) ([]byte, error) {
	if in.left != nil {
		return in.left.Hash(store)
	}
	return zerosHash[:], nil
}

func (in *inner) rhash(store *Store) ([]byte, error) {
	if in.right != nil {
		return in.right.Hash(store)
	}
	return zerosHash[:], nil
}

func (in *inner) load_partial(store *Store) error {
	if in.loaded_partial { // if inner is loaded partially, load it fully now
		return in.loadinnerfromstore(store)
	}
	return nil
}

func (in *inner) Hash(store *Store) ([]byte, error) {
	if in.loaded_partial { // if leaf is loaded partially, load it fully now
		if err := in.loadinnerfromstore(store); err != nil {
			return nil, err
		}
	}

	if len(in.hash) > 0 {
		return in.hash, nil
	}

	var buf [2*HASHSIZE_BYTES + 1]byte
	buf[0] = innerNODE

	var lhash, rhash []byte
	var err error
	if lhash, err = in.lhash(store); err == nil {
		copy(buf[1:], lhash)
		if rhash, err = in.rhash(store); err == nil {
			copy(buf[1+HASHSIZE_BYTES:], rhash)

			hash := sum(buf[:])
			in.hash = append(in.hash[:0], hash[:]...)

			return in.hash, nil
		}
	}

	return nil, err
}

func (in *inner) Position() (uint32, uint32) {
	return in.findex, in.fpos
}

// all puts must be checked with deduplication and skipped if duplicate
func (in *inner) Insert(store *Store, nodes ...*leaf) error {
	if err := in.load_partial(store); err != nil { // if inner node is loaded partially, load it fully now
		return err
	}
	in.dirty = true       // mark node as dirty
	in.hash = in.hash[:0] // cleanup old hash
	for _, n := range nodes {
		if err := in.insert(store, n); err != nil {
			return err
		}
	}
	return nil
}

// insert a node recursively till it gets inserted at the correct position
func (in *inner) insert(store *Store, n *leaf) error {
	if isBitSet(n.keyhash[:], uint(in.bit)) {
		if in.right == nil { // if right node is dead end, we are done
			in.right = n
			return nil
		}
		switch tmp := in.right.(type) { // if right node is not dead end
		case *inner: // if its inner node, recursively insert  the node
			return tmp.Insert(store, n)
		case *leaf: // below case inserts or overwrites existing value, which dropping chains  of long length
			// TODO, since the leaf is already stored, we just need the new inner nodes and thus change only the pointer
			// above optimization will be worthy enough for the slight complexity it creates
			// but it is todo
			if tmp.loaded_partial { // if leaf is loaded partially, load it fully now
				if err := tmp.loadfullleaffromstore(store); err != nil {
					return err
				}
			}
            if (tmp.keyhash[0] == n.keyhash[0] && tmp.keyhash == n.keyhash) || in.bit == lastBit { // if its last node, we are overwriting data, so do it, old versions will be accessible using old roots
				return tmp.Put(store, n.keyhash, n.value)
			}

			in.right = newInner(in.bit + 1) //  otherwise we have enough slack, insert the node, by creating new inner node,
			return in.right.(*inner).Insert(store, tmp, n)
			//	default:	panic("unknown node type")
		}
	}
	//if in.left == nil { 	}loadfullleaffromstore
	switch tmp := in.left.(type) { // if right node is not dead end
	case *inner: // if its inner node, recursively insert  the node
		return tmp.Insert(store, n)
	case *leaf: // below case inserts or overwrites existing value, which dropping chains  of long length
		if tmp.loaded_partial { // if leaf is loaded partially, load it fully now
			if err := tmp.loadfullleaffromstore(store); err != nil {
				return err
			}
		}
        if (tmp.keyhash[0] == n.keyhash[0] && tmp.keyhash == n.keyhash) || in.bit == lastBit { // if its last node, we are overwriting data, so do it, old versions will be accessible using old roots
			return tmp.Put(store, n.keyhash, n.value)
		}
		in.left = newInner(in.bit + 1) //  otherwise we have enough slack, insert the node
		return in.left.(*inner).Insert(store, tmp, n)

	default:
		in.left = n // if left node is dead end, we are done, this is nil case
		return nil
		//default:	panic("unknown node type")
	}

}

func (in *inner) Get(store *Store, keyhash [HASHSIZE]byte) ([]byte, error) {
	if err := in.load_partial(store); err != nil { // if inner node is loaded partially, load it fully now
		return nil, err
	}

	if isBitSet(keyhash[:], uint(in.bit)) {
		if in.right == nil {
			return nil, xerrors.Errorf("%w: right dead end at %d. keyhash %x", ErrNotFound, in.bit, keyhash)
		}
		// we need to fut
		return in.right.Get(store, keyhash)
	}
	if in.left == nil {
		return nil, xerrors.Errorf("%w: left dead end at %d. keyhash %x", ErrNotFound, in.bit, keyhash)
	}
	return in.left.Get(store, keyhash)
}

// leafs return nil,false, inner returns nil, false if both children are present or absent, if single child is present, it is returned
// nodes can only be collapsed, if it's an end leaf node, if the chain hangs lower, keep it hanging
func isOnlyChildleaf(n node) (node, bool) {
	switch v := n.(type) { // draw left  branch
	case nil:
		return nil, false
	case *inner:
		if (v.left != nil && v.right != nil) || (v.left == nil && v.right == nil) {
			return nil, false
		}
		if v.left != nil {
			if getNodeType(v.left) == leafNODE {
				return v.left, true
			}
			return nil, false
		} else {
			if getNodeType(v.right) == leafNODE {
				return v.right, true
			}
			return nil, false
		}
	case *leaf:
		return nil, false
	default:
		panic("unknown node type")
	}

}

// todo we need to take care to prune single branches to achieve same root hash insertion/deletion
// the returns are in this order empty, changed, err
func (in *inner) Delete(store *Store, keyhash [HASHSIZE]byte) (bool, bool, error) {
	if err := in.load_partial(store); err != nil { // if inner node is loaded partially, load it fully now
		return false, false, err
	}
	if isBitSet(keyhash[:], uint(in.bit)) {
		if in.right == nil {
			return false, false, nil
		}
		empty, changed, err := in.right.Delete(store, keyhash)
		if err != nil {
			return false, false, err
		}
		if changed {
			in.dirty = true
			in.hash = in.hash[:0]
		}
		if empty {
			in.right = nil
			return in.isEmpty(), changed, nil
		}

		if n, single := isOnlyChildleaf(in.right); single {
			in.right = n
			return false, changed, nil
		}
		return false, changed, nil
	}
	if in.left == nil {
		return false, false, nil
	}
	empty, changed, err := in.left.Delete(store, keyhash)
	if err != nil {
		return false, false, err
	}
	if changed {
		in.dirty = true
		in.hash = in.hash[:0]
	}
	if empty {
		in.left = nil
		return in.isEmpty(), changed, nil
	}
	if n, single := isOnlyChildleaf(in.left); single {
		in.left = n
		return false, changed, nil
	}
	return false, changed, nil
}

func (in *inner) loadinnerfromstore(store *Store) error { // loading leaf from store
	if in.findex <= 0 && in.fpos <= 0 {
		return xerrors.Errorf("Invalid findex %d fpos %d", in.findex, in.fpos)
	}
	var buf [MINBLOCK]byte

	read_count, err := store.read(in.findex, in.fpos, buf[:]) // atleast  children hashes will be available in this read
	if err != nil && !xerrors.Is(err, io.EOF) {
		return err
	}

	err = in.Unmarshal(buf[:read_count])
	in.loaded_partial = false
	return err
}

func (in *inner) Prove(store *Store, keyhash [HASHSIZE]byte, proof *Proof) error {

	var err error
	if err = in.load_partial(store); err == nil { // if inner node is loaded partially, load it fully now

		proof.version = 1

		if isBitSet(keyhash[:], uint(in.bit)) {
			var lhash []byte
			if lhash, err = in.lhash(store); err == nil {
				proof.addTrace(lhash)
				if in.right != nil {
					return in.right.Prove(store, keyhash, proof)
				}
				proof.addDeadend()
				//return nil
			}
			return err
		}

	}

	var rhash []byte
	if rhash, err = in.rhash(store); err == nil {
		proof.addTrace(rhash)
		if in.left != nil {
			return in.left.Prove(store, keyhash, proof)
		}
		proof.addDeadend()
	}
	return err

}

// minimum size is 3 bytes
func (in *inner) MarshalTo(store *Store, buf []byte, bucket string) (int, error) {
	buf[1] = getNodeType(in.left)  // 1
	buf[2] = getNodeType(in.right) // 1 + 1
	done := 3

	var errors []error

	if in.bit == 0 { // it's a root node so write current and previous version number also
		tsize := binary.PutUvarint(buf[done:], in.version_current) // current version
		done += tsize
		tsize = binary.PutUvarint(buf[done:], in.version_previous) // previous version
		done += tsize
		tsize = binary.PutUvarint(buf[done:], uint64(len(bucket))) // bucket name length
		done += tsize
		done += copy(buf[done:], []byte(bucket)) // write bucket name

	}

	switch getNodeType(in.left) {
	case nullNODE: // no more space needed
	case innerNODE, leafNODE:
		tsize := binary.PutUvarint(buf[done:], uint64(in.left_findex)) // max 10 bytes, but expecting 2 bytes for few years
		done += tsize
		tsize = binary.PutUvarint(buf[done:], uint64(in.left_fpos)) // max 10 bytes, but expecting 5 bytes
		done += tsize

		lhash, err := in.lhash(store)
		errors = append(errors, err)
		done += copy(buf[done:], lhash) // insert left hash

	}
	switch getNodeType(in.right) {
	case nullNODE: // no more space needed
	case innerNODE, leafNODE:
		tsize := binary.PutUvarint(buf[done:], uint64(in.right_findex)) // max 10 bytes, but expecting 2 bytes for few years
		done += tsize
		tsize = binary.PutUvarint(buf[done:], uint64(in.right_fpos)) // max 10 bytes, but expecting 5 bytes
		done += tsize
		rhash, err := in.rhash(store)
		errors = append(errors, err)
		done += copy(buf[done:], rhash) // insert right hash
	}

	buf[0] = byte(done) // prepend with length

	for i := range errors {
		if errors[i] != nil {
			return 0, errors[i]
		}
	}

	return done, nil

}

func parse_node(level byte, nodetype byte, buf []byte) (node, int, error) {
	var done, tsize int
	var tmp uint64

	switch nodetype { // load the left side node
	case nullNODE: // nothing to do
		return nil, 0, nil
	case innerNODE:
		left := newInner(level + 1) // increase bit level
		left.dirty = false
		left.loaded_partial = true
		tmp, tsize = binary.Uvarint(buf[done:])
		if tsize <= 0 || tmp > math.MaxUint32 {
			return nil, 0, xerrors.Errorf("Probably data corruption, we current do not support than 4 billion files")
		}
		left.findex = uint32(tmp)
		done += tsize

		tmp, tsize = binary.Uvarint(buf[done:])
		if tsize <= 0 || tmp > math.MaxUint32 {
			return nil, 0, xerrors.Errorf("Probably data corruption, we current do not support  file pos more than 4GiB")
		}
		left.fpos = uint32(tmp)
		done += tsize

		if len(buf) < done+HASHSIZE {
			return nil, 0, xerrors.Errorf("Probably data corruption, input buffer has incomplete data")
		}

		left.hash = append(left.hash_backer[:0], buf[done:done+HASHSIZE]...)
		done += HASHSIZE

		return left, done, nil

	case leafNODE:

		//fmt.Printf("parsing leaf bytes %x\n", buf[:])
		left := &leaf{loaded_partial: true} // hash will be refilled below
		left.dirty = false
		left.loaded_partial = true
		tmp, tsize = binary.Uvarint(buf[done:]) // max 5 bytes, but expecting 2 bytes
		if tsize <= 0 || tmp > math.MaxUint32 {
			return nil, 0, xerrors.Errorf("Probably data corruption, we current do not support than 4 billion files")
		}
		left.findex = uint32(tmp)
		done += tsize

		tmp, tsize = binary.Uvarint(buf[done:]) // max 10 bytes, but expecting 5 bytes
		if tsize <= 0 || tmp > math.MaxUint32 {
			return nil, 0, xerrors.Errorf("Probably data corruption, we current do not support  file pos more than 4GiB")
		}
		left.fpos = uint32(tmp)
		done += tsize

		if len(buf) < done+HASHSIZE {
			return nil, 0, xerrors.Errorf("Probably data corruption, input buffer has incomplete data")
		}

		copy(left.hash[:], buf[done:done+HASHSIZE])
		copy(left.hash_check[:], buf[done:done+HASHSIZE]) // this will be used later on verify

		left.leaf_init = true

		done += HASHSIZE

		return left, done, nil

	default:
		return nil, 0, xerrors.Errorf("Probably data corruption, unknown node type")

	}
}

// first byte is skipped and processed elsewhere
func (in *inner) Unmarshal(buf []byte) (err error) {

	/*length, length_bytes := binary.Varint(buf)
	if length_bytes <0 || length <= 0 ||  len(buf) < (int(length) + length_bytes)  {
		panic("inner node length cannot be zero")
		return fmt.Errorf("inner node invalid length")
	}
	*/
	if len(buf) < 3 {
		return xerrors.Errorf("0 byte buffer cannot be Unmarshalled")
	}

	length_bytes := 1
	length := int(uint(buf[0]))
	_ = length

	buf = buf[length_bytes:]

	done := 2
	var tsize int
	if in.bit == 0 { // it's a root node so write current and previous version number also
		in.version_current, tsize = binary.Uvarint(buf[done:]) // current version
		done += tsize
		in.version_previous, tsize = binary.Uvarint(buf[done:]) // previous version
		done += tsize
		blen, tsize := binary.Uvarint(buf[done:])
		done += tsize

		//var lbuf[BUCKET_NAME_LIMIT]byte
		//copy(lbuf[:], buf[done : done+int(blen)] )
		//bucketname = string(lbuf[:blen])
		in.bucket_name = append(in.bucket_name[:0], buf[done:done+int(blen)]...)

		done += int(blen)
	}

	in.left, tsize, err = parse_node(in.bit, buf[0], buf[done:])
	if err != nil {
		return
	}
	done += tsize

	in.right, tsize, err = parse_node(in.bit, buf[1], buf[done:])
	if err != nil {
		return
	}

	return
}
