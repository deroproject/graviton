//+build ignore

package graviton

import "os"
import "fmt"
import "bufio"

// this function will export the tree to a dor graph file to understand any issues
func (t *Tree) Graph(fname string) (err error) {

	f, err := os.Create(fname)
	if err != nil {
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	w.WriteString("digraph graviton_graph { \n")
	defer w.WriteString(" \n}\n")

	t.graph(t.root, w)
	return nil
}
func (t *Tree) graph(cnode node, w *bufio.Writer) {
	var err error
	switch node := cnode.(type) {
	case *inner:
		if node.loaded_partial { // if node is loaded partially, load it fully now
			if err = node.loadinnerfromstore(t.store); err != nil {
				return
			}
		}

		w.WriteString(fmt.Sprintf("node [ fontsize=12 style=filled ]\n{\n"))
		hash, _ := node.Hash(t.store)
		w.WriteString(fmt.Sprintf("L%x  [ fillcolor=%s label = \"L%x\"  ];\n", hash, "red", hash))
		w.WriteString(fmt.Sprintf("}\n"))

		left, right := node.left, node.right

		if right != nil {
			rhash, _ := right.Hash(t.store)
			w.WriteString(fmt.Sprintf("L%x -> L%x ;\n", hash, rhash))
			t.graph(right, w) // descend further without any option
		}

		if left != nil {
			lhash, _ := left.Hash(t.store)
			w.WriteString(fmt.Sprintf("L%x -> L%x ;\n", hash, lhash))
			t.graph(left, w) // descend further without any option
		}

		return
	case *leaf:
		if node.loaded_partial { // if leaf is loaded partially, load it fully now
			if err = node.loadfullleaffromstore(t.store); err != nil {
				return
			}
		}
		w.WriteString(fmt.Sprintf("node [ fontsize=12 style=filled ]\n{\n"))
		hash, _ := node.Hash(t.store)
		keyhash := sum(node.key)
		w.WriteString(fmt.Sprintf("L%x  [ fillcolor=%s label = \"L%x   %x\"  ];\n", hash, "green", hash, keyhash))
		w.WriteString(fmt.Sprintf("}\n"))
		//return node.key, node.value, nil
	default:
		panic("unknown node type, corruption")
		return
	}
}
