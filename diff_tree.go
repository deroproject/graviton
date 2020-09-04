package graviton

//import "io"
//import "fmt"

//import "errors"
//import "os"
import "bytes"

//import "sync"
//import "encoding/binary"

//import "golang.org/x/xerrors"

//Cursor represents an iterator that can traverse over all key/value pairs in a tree in hash sorted order.
//Cursors can be obtained from a tree and are valid as long as the tree is valid.
//Keys and values returned from the cursor are only valid for the life of the transaction.
//Changing tree (before committing) while traversing with a cursor may cause it to be invalidated and return unexpected keys and/or values. You must reposition your cursor after mutating data.
type diffTree struct {
	base_tree, head_tree *Tree
}

// All changes are reported of this type, deleted, modified, inserted
type DiffHandler func(k, v []byte)

// This function can be used to diff 2 trees and thus find all the keys which have been deleted, modified, inserted.
// The algorithm is linear time in the number of changes. Eg. a tree with billion KVs can be diffed with parent almost instantaneously.
// Todo : API redesign, should we give only keys, since values can be obtained later on !!
func Diff(base_tree, head_tree *Tree, deleted, modified, inserted DiffHandler) (err error) {
	dt := diffTree{base_tree: base_tree, head_tree: head_tree}
	return dt.changes_internal(base_tree.root, head_tree.root, deleted, modified, inserted)
}

// extract changes one bye one
func (dt *diffTree) changes_internal(base_node, head_node *inner, deleted, modified, inserted DiffHandler) (err error) {

	var base_hash, head_hash []byte
	if base_hash, err = base_node.Hash(dt.base_tree.store); err == nil {
		if head_hash, err = head_node.Hash(dt.head_tree.store); err == nil {

			if bytes.Compare(base_hash, head_hash) == 0 {

				return nil
			}
		}
	}

	if err != nil {
		return
	}

	if err = dt.compare_nodes(base_node.left, head_node.left, deleted, modified, inserted); err != nil {
		return
	}
	return dt.compare_nodes(base_node.right, head_node.right, deleted, modified, inserted)
}

func (dt *diffTree) compare_nodes(base_node, head_node node, deleted, modified, inserted DiffHandler) (err error) {

	var k, v []byte
	if base_node == nil && head_node == nil { // nothing to do on left side
		return
	}

	if base_node == nil && head_node != nil { // all the head nodes were added
		c := dt.head_tree.Cursor()
		for k, v, err = c.next_internal(head_node, false); err == nil; k, v, err = c.Next() {
			if inserted != nil {
				inserted(k, v)
			}
		}
		if err == ErrNoMoreKeys {
			return nil
		}
		//return err

	} else if base_node != nil && head_node == nil { // all the base nodes were deleted
		c := dt.base_tree.Cursor()
		for k, v, err = c.next_internal(base_node, false); err == nil; k, v, err = c.Next() {
			if deleted != nil {
				deleted(k, v)
			}
		}
		if err == ErrNoMoreKeys {
			return nil
		}
		//return err

	} else {

		// both sides are not nil
		base_type := getNodeType(base_node)
		head_type := getNodeType(head_node)

		if err = base_node.load_partial(dt.base_tree.store); err != nil {
			return err
		}
		if err = head_node.load_partial(dt.head_tree.store); err != nil {
			return err
		}

		if base_type == innerNODE && head_type == innerNODE {
			return dt.changes_internal(base_node.(*inner), head_node.(*inner), deleted, modified, inserted)
		} else if base_type == leafNODE && head_type == leafNODE {

			// if both leafs are different, process else leafs are same nothing to do

			var base_hash, head_hash []byte
			if base_hash, err = base_node.Hash(dt.base_tree.store); err == nil {
				if head_hash, err = head_node.Hash(dt.head_tree.store); err == nil {

					if bytes.Compare(base_hash, head_hash) != 0 {

						base_leaf := base_node.(*leaf)
						head_leaf := head_node.(*leaf)

						// if keys are same, then values are different or values are updates
						if bytes.Compare(base_leaf.key, head_leaf.key) == 0 {
							if modified != nil {
								//fmt.Printf("110 key %s basevalue %s  headvalue %s\n", string(base_leaf.key), string(base_leaf.value), string(head_leaf.value))
								modified(head_leaf.key, head_leaf.value)
							}
						} else {
							// base leaf was deleted
							// head leaf was inserted
							if deleted != nil {
								deleted(base_leaf.key, base_leaf.value)
							}
							if inserted != nil {
								inserted(head_leaf.key, head_leaf.value)

							}
						}
					}
					return nil

				}

			}

			// we are if any hash err and we return it
			//return err

			// one of nodes is inner and one of the node is leaf
		} else if base_type == innerNODE { // base type is inner node, head type is leaf node
			head_leaf := head_node.(*leaf)
			head_leaf.Hash(dt.head_tree.store) // if partial complete the node

			// check whether base tree contains this node
			v, err := dt.base_tree.Get(head_leaf.key)
			if err == nil { // key was found
				if bytes.Compare(v, head_leaf.value) == 0 { // same key,value exist, we must not report it

				} else { // same key, but value changed, we must a modification
					if modified != nil {
						//fmt.Printf("140 key %s basevalue %s  headvalue %s\n", string(head_leaf.key), string(v), string(head_leaf.value))
						modified(head_leaf.key, head_leaf.value)
					}
				}
			} else { // either key was not found or some error occurred
				if inserted != nil {
					inserted(head_leaf.key, head_leaf.value)
				}
			}

			err = nil

			// now the entire base tree must be searched, limited at specific node, and the head leaf key skipped
			c := dt.base_tree.Cursor()
			for k, v, err = c.next_internal(base_node, false); err == nil; k, v, err = c.Next() {
				if bytes.Compare(k, head_leaf.key) != 0 {
					if deleted != nil {
						deleted(k, v)
					}
				}
			}
			if err == ErrNoMoreKeys {
				return nil
			}
			//return err

		} else { // base type is leaf node, head type is inner node

			base_leaf := base_node.(*leaf)
			base_leaf.Hash(dt.base_tree.store) // if partial complete the node

			// check whether base tree contains this node
			v, err := dt.head_tree.Get(base_leaf.key)
			if err == nil { // key was found
				if bytes.Compare(v, base_leaf.value) == 0 { // same key,value exist, we must not report it

				} else { // same key, but value changed, we must a modification
					if modified != nil {
						//fmt.Printf("182 key %s basevalue %s  headvalue %s\n", string(base_leaf.key), string(v), string(base_leaf.value))
						modified(base_leaf.key, v)
					}
				}
			} else { // either key was not found or some error occurred
				if deleted != nil {
					deleted(base_leaf.key, base_leaf.value)
				}
			}

			err = nil

			// now the entire base tree must be searched, limited at specific node, and the head leaf key skipped
			c := dt.head_tree.Cursor()
			for k, v, err = c.next_internal(head_node, false); err == nil; k, v, err = c.Next() {
				if bytes.Compare(k, base_leaf.key) != 0 {
					if inserted != nil {
						inserted(k, v)
					}
				}
			}
			if err == ErrNoMoreKeys {
				return nil
			}
			//return err
		}

	}

	return err
}
