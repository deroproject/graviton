package graviton

import "io"
import "bytes"
import "fmt"
import "encoding/binary"
import "golang.org/x/xerrors"

var gET_CHECKED bool = true // all gets go through value checks

type leaf struct {
	findex, fpos uint32

	key     []byte
	keybuf  [MINBLOCK]byte
	keyhash [HASHSIZE]byte

	hash_check [HASHSIZE]byte // used to verify check
	hash       [HASHSIZE]byte

	leaf_init bool

	value []byte

	dirty bool
	//dirtyhash      bool
	loaded_partial bool
}

func newLeaf(keyhash [HASHSIZE]byte, key, value []byte) *leaf {
	key_copy := make([]byte, len(key))
	copy(key_copy, key[:])
	value_copy := make([]byte, len(value))
	copy(value_copy, value[:])

	l := &leaf{
		dirty:   true, // new leaf is by default dirty
		keyhash: keyhash,
		value:   value_copy,
	}
	l.key = append(l.keybuf[:0], key...)

	rst := sum(l.value)
	copy(l.hash_check[:], leafHash(l.keyhash[:], rst[:]))
	copy(l.hash[:], l.hash_check[:])

	l.leaf_init = true

	//fmt.Printf("leafhash new %x key '%s' value '%s'\n", l.hash_check, string(key), string(value))
	return l
}

func leafHash(hkey, hvalue []byte) []byte {
	rst := make([]byte, 0, HASHSIZE)
	h := hasher()
	h.Write([]byte{leafNODE})
	h.Write(hkey)
	h.Write(hvalue)
	rst = h.Sum(rst)
	return rst
}

func (l *leaf) Hash(store *Store) ([]byte, error) {
	if l.loaded_partial { // if leaf is loaded partially, load it fully now
		if err := l.loadfullleaffromstore(store); err != nil {
			return nil, err
		}
	}
	return l.hash[:], nil
}

func (l *leaf) isDirty() bool {
	return l.dirty
}
func (l *leaf) Position() (uint32, uint32) {
	return l.findex, l.fpos
}

// this always assummes that keyhash already matches to new keyhash
// this function is only used once , in node_inner.go insert
func (l *leaf) Put(store *Store, keyhash [HASHSIZE]byte, value []byte) error {
	if l.loaded_partial { // if leaf is loaded partially, load it fully now
		if err := l.loadfullleaffromstore(store); err != nil {
			return err
		}
	}
	// overwrite created new branch. Old versions are all accessible using previous root
	l.value = value
	rst := sum(l.value)
	copy(l.hash[:], leafHash(l.keyhash[:], rst[:])) // use hash of key and hash of value
	copy(l.hash_check[:], l.hash[:])
	l.dirty = true
	l.findex, l.fpos = 0, 0
	return nil
}

// should we return a copy
func (l *leaf) Get(store *Store, keyhash [HASHSIZE]byte) ([]byte, error) {
	if l.loaded_partial { // if leaf is loaded partially, load it fully now
		if err := l.loadfullleaffromstore(store); err != nil {
			return nil, err
		}
	}
	if l.keyhash == keyhash {
		return l.value, nil
	}

	return nil, xerrors.Errorf("%w: collision, keyhash %x not found", ErrNotFound, keyhash)
}

func (l *leaf) Delete(store *Store, keyhash [HASHSIZE]byte) (bool, bool, error) {
	if l.loaded_partial { // if leaf is loaded partially, load it fully now
		if err := l.loadfullleaffromstore(store); err != nil {
			return false, false, err
		}
	}
	match := l.keyhash == keyhash
	return match, match, nil
}

func (l *leaf) load_partial(store *Store) error {
	if l.loaded_partial { // if leaf is loaded partially, load it fully now
		return l.loadfullleaffromstore(store)
	}
	return nil
}

func (l *leaf) loadfullleaffromstore(store *Store) error { // loading leaf from store
	//fmt.Printf("loading leaf findex %d fpos %d\n", l.findex, l.fpos)
	if l.findex <= 0 && l.fpos <= 0 {
		return xerrors.Errorf("Invalid findex %d fpos %d", l.findex, l.fpos)
	}
	var buf_array [4 * MINBLOCK]byte
	buf := buf_array[:]

read_again:

	_, err := store.read(l.findex, l.fpos, buf[:]) // atleast keylen, key, valuelen will be available in this read, if value is small,it's also available
	if err != nil && err != io.EOF {
		return err
	}

	var done int

	l.key = l.keybuf[:0]
	l.value = l.value[:0]

	if value, bytecount := binary.Uvarint(buf[:]); bytecount > 0 {
		l.key = append(l.keybuf[:0], buf[bytecount:uint64(bytecount)+value]...)
		done += bytecount + int(value)
	} else {
		return xerrors.Errorf("invalid key size")
	}

	if value, bytecount := binary.Uvarint(buf[done:]); bytecount > 0 {

		if done+bytecount+int(value) > len(buf) {
			buf = make([]byte, done+bytecount+int(value), done+bytecount+int(value))
			goto read_again
		}
		//fmt.Printf("value %d bytecount %d done %d len(buf) %d\n", value,bytecount, done, len(buf))
		l.value = append(l.value, buf[done+bytecount:done+bytecount+int(value)]...)
		done += bytecount + int(value)
	} else {
		return xerrors.Errorf("invalid value size")
	}

	// time for data integrity

	l.keyhash = sum(l.key)

	// we also need to calculate hash, see whether it matches with what is stored

	rst := sum(l.value)
	copy(l.hash[:], leafHash(l.keyhash[:], rst[:])) // use hash of key and hash of value

	if gET_CHECKED {
		if bytes.Compare(l.hash_check[:], l.hash[:]) != 0 {

			//fmt.Printf("hash_check %x hash %x keyhash %x\n", l.hash_check, l.hash, l.keyhash)

			return fmt.Errorf("Key/Value data Corruption, key '%x'", l.key)

		}
	}

	l.loaded_partial = false

	return nil
}

func (l *leaf) Prove(store *Store, keyhash [HASHSIZE]byte, proof *Proof) error {
	if l.loaded_partial { // if leaf is loaded partially, load it fully now
		if err := l.loadfullleaffromstore(store); err != nil {
			return err
		}
	}
	if l.keyhash == keyhash {
		proof.addValue(l.value)
		return nil
	}
	rst := sum(l.value)
	proof.addCollision(l.keyhash[:], rst[:])
	return nil
}
