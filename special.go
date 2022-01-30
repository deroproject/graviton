package graviton

//import "io"
//import "math"

import "fmt"
import "bytes"
import "golang.org/x/xerrors"

// this file contains some functions ( to extend read-only api). these apis are used in the dero blockchain.

func Sum(key []byte) [HASHSIZE]byte {
	return sum(key)
}

// we have a key and need to get both the key,value
func (t *Tree) GetKeyValueFromKey(key []byte) (int, []byte, []byte, error) {
	return t.root.GetKeyValue(t.store, sum(key), 256, 0)
}

// we only have a keyhash and need to get both the key,value
func (t *Tree) GetKeyValueFromHash(keyhashc []byte) (int, []byte, []byte, error) {
	var keyhash [HASHSIZE]byte
	if len(keyhashc) <= 0 || len(keyhashc) > HASHSIZE {
		return 0, nil, nil, fmt.Errorf("keyhashc must be atleast 1 byte and less than 33 bytes, len=%d", len(keyhashc))
	}
	copy(keyhash[:], keyhashc)

	return t.root.GetKeyValue(t.store, keyhash, len(keyhashc)*8, 0)
}

func (in *inner) GetKeyValue(store *Store, keyhash [HASHSIZE]byte, valid_bit_count, used_bit_count int) (int, []byte, []byte, error) {
	if err := in.load_partial(store); err != nil { // if inner node is loaded partially, load it fully now
		return used_bit_count, nil, nil, err
	}

	if used_bit_count > valid_bit_count || valid_bit_count <= 0 {
		return used_bit_count, nil, nil, xerrors.Errorf("%w: right dead end at %d. keyhash %x", ErrNotFound, in.bit, keyhash)
	}

	if isBitSet(keyhash[:], uint(in.bit)) {
		if in.right == nil {
			return used_bit_count, nil, nil, xerrors.Errorf("%w: right dead end at %d. keyhash %x", ErrNotFound, in.bit, keyhash)
		}
		switch in.right.(type) { // draw left  branch
		case *inner:
			return in.right.(*inner).GetKeyValue(store, keyhash, valid_bit_count, used_bit_count+1)
		case *leaf:
			return in.right.(*leaf).GetKeyValue(store, keyhash, valid_bit_count, used_bit_count+1)
		default:
			panic("unknown node type")
		}

	}
	if in.left == nil {
		return used_bit_count, nil, nil, xerrors.Errorf("%w: left dead end at %d. keyhash %x", ErrNotFound, in.bit, keyhash)
	}
	switch in.left.(type) { // draw left  branch
	case *inner:
		return in.left.(*inner).GetKeyValue(store, keyhash, valid_bit_count, used_bit_count+1)
	case *leaf:
		return in.left.(*leaf).GetKeyValue(store, keyhash, valid_bit_count, used_bit_count+1)
	default:
		panic("unknown node type")
	}
}

// should we return a copy
func (l *leaf) GetKeyValue(store *Store, keyhash [HASHSIZE]byte, valid_bit_count, used_bit_count int) (int, []byte, []byte, error) {
	if l.loaded_partial { // if leaf is loaded partially, load it fully now
		if err := l.loadfullleaffromstore(store); err != nil {
			return used_bit_count, nil, nil, err
		}
	}

	if bytes.Compare(l.keyhash[:valid_bit_count/8], keyhash[:valid_bit_count/8]) == 0 {
		return used_bit_count, l.key, l.value, nil
	}

	return used_bit_count, nil, nil, xerrors.Errorf("%w: collision, keyhash %x not found, inram hash %x, used_bit_count %d", ErrNotFound, keyhash,l.keyhash,used_bit_count)
}

// sets a root for the cursor, so the cursor visits only a specific prefix keys
func (c *Cursor) SpecialFirst(section []byte, validbits uint) (k, v []byte, err error) {
	loop_node := node(c.tree.root) // we always start at root node

	donebits := uint(0)

	if validbits >= 256 {
		err = fmt.Errorf("invalid valid bits %d", validbits)
		return
	}

	if validbits == 0 {
		return c.First()
	}

	// the function is iterative and not recursive
	for {
		switch node := loop_node.(type) {
		case *inner:
			if node.loaded_partial { // if node is loaded partially, load it fully now
				if err = node.loadinnerfromstore(c.tree.store); err != nil {
					return
				}
			}

			left, right := node.left, node.right
			if isBitSet(section, donebits) { // 1 is right
				if right == nil {
					err = ErrNoMoreKeys
					return
				}
				loop_node = right
			} else { //0 is left
				if left == nil {
					err = ErrNoMoreKeys
					return
				}
				loop_node = left
			}
			donebits++

			if donebits < validbits {
				continue
			} else if donebits == validbits {
				return c.next_internal(loop_node, false)
			}

			// we can only reach here if a tree has both left,right nil, ie an empty tree
			err = ErrNoMoreKeys
			return

		case *leaf:
			err = ErrNoMoreKeys
			return
		default:
			return k, v, fmt.Errorf("unknown node type, corruption")
		}
	}
}
