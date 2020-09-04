package graviton

import "fmt"

//Cursor represents an iterator that can traverse over all key/value pairs in a tree in hash sorted order.
//Cursors can be obtained from a tree and are valid as long as the tree is valid.
//Keys and values returned from the cursor are only valid for the life of the transaction.
//Changing tree (before committing) while traversing with a cursor may cause it to be invalidated and return unexpected keys and/or values. You must reposition your cursor after mutating data.
type Cursor struct {
	tree *Tree

	node_path []*inner
	left      []bool
}

// get Cursor which is used as an iterator that can traverse over all key/value pairs in a tree in hash sorted order.
func (t *Tree) Cursor() Cursor {
	return Cursor{tree: t}
}

// First moves the cursor to the first item in the tree and returns its key and value. If the tree is empty then an error is returned. The returned key and value are only valid for the life of the tree.
func (c *Cursor) First() (k, v []byte, err error) {
	// the function is iterative and not recursive
	return c.next_internal(node(c.tree.root), false)
}

// Last moves the cursor to the last item in the bucket and returns its key and value. If the tree is empty then an error is returned. The returned key and value are only valid for the life of the tree.
func (c *Cursor) Last() (k, v []byte, err error) {
	// the function is iterative and not recursive
	return c.next_internal(node(c.tree.root), true)
}

// this function will descend and reach the next or previous value
func (c *Cursor) next_internal(loop_node node, reverse bool) (k, v []byte, err error) {
	for {
		switch node := loop_node.(type) {
		case *inner:
			if node.loaded_partial { // if node is loaded partially, load it fully now
				if err = node.loadinnerfromstore(c.tree.store); err != nil {
					return
				}
			}

			left, right := node.left, node.right
			if reverse {
				left, right = right, left
			}

			if left != nil {
				c.node_path = append(c.node_path, node)
				c.left = append(c.left, true == !reverse)
				loop_node = left
				continue // we must descend further
			}

			if right != nil {
				c.node_path = append(c.node_path, node)
				c.left = append(c.left, false == !reverse)
				loop_node = right
				continue // we must descend further
			}

			// we can only reach here if a tree has both left,right nil, ie an empty tree
			err = ErrNoMoreKeys
			return
			break

		case *leaf:
			if node.loaded_partial { // if leaf is loaded partially, load it fully now
				if err = node.loadfullleaffromstore(c.tree.store); err != nil {
					return
				}
			}
			return node.key, node.value, nil
		default:
			return k, v, fmt.Errorf("unknown node type, corruption")
		}
	}
}

// Next moves the cursor to the next item in the tree and returns its key and value.If the tree is empty then an error is returned.If the cursor is at the end of the tree, then an error is returned. The returned key and value are only valid for the life of the tree.
func (c *Cursor) Next() (k, v []byte, err error) {

try_again:
	if len(c.node_path) == 0 {
		err = ErrNoMoreKeys
		return
	}
	cur_node_index := len(c.node_path) - 1

	if !c.left[cur_node_index] || c.node_path[cur_node_index].right == nil { // since we are a right node, we must back track one node
		c.node_path = c.node_path[:cur_node_index]
		c.left = c.left[:cur_node_index]
		goto try_again
	}
	// we are here means we are on a left node, lets check the right node
	c.left[cur_node_index] = false

	if err = c.node_path[cur_node_index].right.load_partial(c.tree.store); err != nil {
		return
	}
	switch node := c.node_path[cur_node_index].right.(type) {
	case *inner:
		return c.next_internal(node, false)
	case *leaf:
		return node.key, node.value, nil

	default:
		return k, v, fmt.Errorf("unknown node type, corruption")
	}
}

// Prev moves the cursor to the prev item in the tree and returns its key and value.If the tree is empty then an error is returned.If the cursor is at the end of the tree, then an error is returned. The returned key and value are only valid for the life of the tree.
func (c *Cursor) Prev() (k, v []byte, err error) {
try_again:
	if len(c.node_path) == 0 {
		err = ErrNoMoreKeys
		return
	}
	cur_node_index := len(c.node_path) - 1
	if c.left[cur_node_index] || c.node_path[cur_node_index].left == nil { // since we are a right node, we must back track one node
		c.node_path = c.node_path[:cur_node_index]
		c.left = c.left[:cur_node_index]
		goto try_again
	}
	// we are here means we are on a right node, lets check the left node
	c.left[cur_node_index] = true

	if err = c.node_path[cur_node_index].left.load_partial(c.tree.store); err != nil {
		return
	}
	switch node := c.node_path[cur_node_index].left.(type) {
	case *inner:
		return c.next_internal(node, true)
	case *leaf:
		return node.key, node.value, nil

	default:

		return k, v, fmt.Errorf("unknown node type, corruption")
	}
}
