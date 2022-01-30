package graviton

import "fmt"
import "math"
import "crypto/rand"


func (t *Tree) GetName() (string) {
	return t.treename
}
// Random returns a random key,value from the tree, provided a tree has keys
// the following are limitations
// a tree containing 0 key, value pairs will return err
// randomness depends on number of keys, if tree contains only 1 value, it will be ported etc
func (t *Tree) Random() (k, v []byte, err error) {
	return t.random(t.root)
}
func (t *Tree) random(cnode node) (k, v []byte, err error) {
	switch node := cnode.(type) {
	case *inner:
		if node.loaded_partial { // if node is loaded partially, load it fully now
			if err = node.loadinnerfromstore(t.store); err != nil {
				return
			}
		}
		left, right := node.left, node.right
		if left != nil && right != nil { // we have an option to choose from left or right randomly
			var rbyte [1]byte
			if _, err = rand.Read(rbyte[:]); err != nil {
				return
			}
			if rbyte[0]&1 == 1 {
				return t.random(right) // descend further
			}
			return t.random(left) // descend further
		}

		if right != nil {
			return t.random(right) // descend further without any option
		}

		if left != nil {
			return t.random(left) // descend further without any option
		}

		// we can only reach here if a tree has both left,right nil, ie an empty tree
		err = ErrNoMoreKeys
		return
	case *leaf:
		if node.loaded_partial { // if leaf is loaded partially, load it fully now
			if err = node.loadfullleaffromstore(t.store); err != nil {
				return
			}
		}
		return node.key, node.value, nil
	default:
		return k, v, fmt.Errorf("unknown node type, corruption")
	}
}

// estimate number of keys that exist in the tree
// very crude but only used for use display
func (t *Tree) KeyCountEstimate() (count int64) {
	c := t.Cursor()

	var depth_array []int
	var floatsum float64
	for _, _, err := c.First(); err == nil; _, _, err = c.Next() {
		floatsum += float64(len(c.node_path))
		depth_array = append(depth_array, len(c.node_path))
		if len(depth_array) >= 20 {
			break
		}
	}
	if len(depth_array) <= 4 {
		return int64(count)
	}
	avg := floatsum / float64(len(depth_array)+1)
	return int64(math.Exp2(avg))
}
