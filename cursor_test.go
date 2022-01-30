package graviton

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func randStr(len int) string {
	buff := make([]byte, len)
	rand.Read(buff)
	return base64.StdEncoding.EncodeToString(buff)
}

// this test various cursor related code
func TestCursor(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	cursor := tree.Cursor()

	first_key, first_value, err := cursor.First()
	require.Error(t, err) // since tree is empty we must receive err

	tree.Put([]byte("key"), []byte("value"))
	first_key, first_value, err = cursor.First()
	require.NoError(t, err) // since tree is not empty we must receive err

	if string(first_key) != "key" || string(first_value) != "value" {
		t.Fatalf("Cursor First failed")
	}

}

// this test various cursor related code
func TestCursor1(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	keyval_reference := map[string]string{}

	for i := 0; i < 100000; i++ {
		key := randStr(60)
		value := randStr(512)
		keyval_reference[key] = value
		tree.Put([]byte(key), []byte(value))
	}

test_committed_tree:
	cursor := tree.Cursor()

	key_copy := map[string]string{}

	var key_array_left_to_right []string
	var key_array_right_to_left []string

	for k, v, err := cursor.First(); err == nil; k, v, err = cursor.Next() {
		key_copy[string(k)] = string(v)
		key_array_left_to_right = append(key_array_left_to_right, string(k))

		//t.Logf("key %s val %s", string(k), string(v))
		if _, ok := keyval_reference[string(k)]; !ok { // make sure spurious keys are not being generated
			t.Fatalf(" missing key")
		}
	}

	if len(keyval_reference) != len(key_copy) { // make sures elements are not duplicated
		t.Fatalf(" missing key %d %d", len(keyval_reference), len(key_copy))
	}

	if tree.IsDirty() == false {
		gv, err = store.LoadSnapshot(0)
		require.NoError(t, err)
		tree, _ = gv.GetTree("root")
		cursor = tree.Cursor()
	}

	key_copy = map[string]string{}
	for k, v, err := cursor.Last(); err == nil; k, v, err = cursor.Prev() {
		key_copy[string(k)] = string(v)
		key_array_right_to_left = append(key_array_right_to_left, string(k))

		//t.Logf("key %s val %s", string(k), string(v))
		if _, ok := keyval_reference[string(k)]; !ok {
			t.Fatalf(" missing key")
		}
	}

	if len(keyval_reference) != len(key_copy) {
		t.Fatalf(" missing key %d %d", len(keyval_reference), len(key_copy))
	}

	for i := range key_array_right_to_left {
		if key_array_left_to_right[i] != key_array_right_to_left[len(key_array_right_to_left)-i-1] {
			t.Fatalf("Cursor corruption in reverse direction")
		}
	}

	if tree.IsDirty() {
		tree.Commit()
		goto test_committed_tree
	}

}

// this test various cursor related code
// this test uses hard coded values based on hash, chnaging the hash will cause this test to fails
// NOTE: this test is dependent on hash function
func TestCursor_error_case(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	keys := map[string]string{}
	for i := 0; i < 50000; i++ {
		key := fmt.Sprintf("%d", i)

		keyhash := sum([]byte(key))

		keyhash_string := fmt.Sprintf("%02b", keyhash[0]>>6)

		if _, ok := keys[keyhash_string]; !ok {
			//t.Logf("%s key %s", keyhash_string, key)

			keys[keyhash_string] = key

			tree.Put([]byte(key), []byte(key))

			if len(keys) == 4 {
				break
			}
		}

	}

	tree.Commit()

	//tree.graph("/tmp/graph.dot")

	gv, err = store.LoadSnapshot(0)
	require.NoError(t, err)
	tree, _ = gv.GetTree("root")
	cursor := tree.Cursor()

	//corrupting root node error
	tree.root.findex = 1000000000
	tree.root.loaded_partial = true
	_, _, err = cursor.First()
	require.Error(t, err)

	tree, _ = gv.GetTree("root")
	cursor = tree.Cursor()
	tree.root.left.(*inner).right.(*leaf).findex = 1000000000
	tree.root.left.(*inner).right.(*leaf).loaded_partial = true
	_, _, err = cursor.Next()
	_, _, err = cursor.Next()
	require.Error(t, err)

	_, _, err = cursor.Last() // last leaf has been corrupted , so trigger this error
	_, _, err = cursor.Prev()
	_, _, err = cursor.Prev()
	require.Error(t, err)

	tree, _ = gv.GetTree("root")
	cursor = tree.Cursor()

	k, v, err := cursor.First()
	tree.root.right.(*inner).findex = 1000000000
	tree.root.right.(*inner).loaded_partial = true

	//t.Logf("k \"%s\" v \"%s\" err \"%s\" %d ", string(k), string(v), err, len(cursor.node_path))
	k, v, err = cursor.Next()
	k, v, err = cursor.Next()
	_ = k
	_ = v

	require.Error(t, err)

	tree, _ = gv.GetTree("root")
	cursor = tree.Cursor()

	k, v, err = cursor.Last()
	tree.root.left.(*inner).findex = 1000000000
	tree.root.left.(*inner).loaded_partial = true

	k, v, err = cursor.Prev()
	k, v, err = cursor.Prev()

	require.Error(t, err)

	tree, err = gv.GetTree("damagedroot")
	require.NoError(t, err)

	tree.root.left = &dummynode{}

	cursor = tree.Cursor()

	k, v, err = cursor.next_internal(tree.root.left, false)
	require.Error(t, err)

}
