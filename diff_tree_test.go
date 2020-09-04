package graviton

import (
	"encoding/base64"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func randString(len int) string {
	buff := make([]byte, len)
	rand.Read(buff)
	return base64.StdEncoding.EncodeToString(buff)
}

// this test various diffing related code
func TestDiffTree(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	keyval_reference := map[string]string{}

	var keys []string

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 100000; i++ {
		key := randString(32)
		value := randString(8)
		keyval_reference[key] = value

		keys = append(keys, key)
		tree.Put([]byte(key), []byte(value))
	}

	tree.Commit() // commit the tree

	insert_map_reference := map[string]string{}
	delete_map_reference := map[string]string{}
	modify_map_reference := map[string]string{}

	insert_map_actual := map[string]string{}
	delete_map_actual := map[string]string{}
	modify_map_actual := map[string]string{}

	for i := 0; i < len(keyval_reference)/3; i++ {
		key := randString(32)
		value := randString(8)
		_ = key
		_ = value
		insert_map_reference[key] = value
		tree.Put([]byte(key), []byte(value))
	}

	for k := range keyval_reference {
		if len(modify_map_reference) < len(keyval_reference)/3 {
			value := randString(8)
			modify_map_reference[k] = value
			tree.Put([]byte(k), []byte(value))
		} else {
			break
		}
	}

	// delete should be after modify
	for len(delete_map_reference) < len(insert_map_reference) {
		rkey := keys[rand.Intn(len(keys))]
		if _, ok := modify_map_reference[rkey]; !ok {
			delete_map_reference[rkey] = keyval_reference[rkey]
			tree.Delete([]byte(rkey))
		}
	}

	tree.Commit() // commit the tree

	gv1, _ := store.LoadSnapshot(1)
	base_tree, _ := gv1.GetTree("root")

	gv2, _ := store.LoadSnapshot(2)
	head_tree, _ := gv2.GetTree("root")

	insert_handler := func(k, v []byte) {
		insert_map_actual[string(k)] = string(v)
	}
	delete_handler := func(k, v []byte) {
		delete_map_actual[string(k)] = string(v)
	}

	modify_handler := func(k, v []byte) {
		//t.Logf("k %s v %s  reference %s" , string(k), string(v) , modify_map_reference[string(k)])
		modify_map_actual[string(k)] = string(v)
	}

	err = Diff(base_tree, head_tree, delete_handler, modify_handler, insert_handler)

	//t.Logf("err  %s",err)

	//t.Logf("delete %d modify %d insert %d", len(delete_map_actual), len(modify_map_actual),  len(insert_map_actual)  )

	if !reflect.DeepEqual(insert_map_reference, insert_map_actual) {
		t.Logf(" insert map reference %d actual  %d", len(insert_map_reference), len(insert_map_actual))
		t.Fatalf("inserts could not be diffed")
	}

	if !reflect.DeepEqual(delete_map_reference, delete_map_actual) {
		t.Logf(" delete map count %d", len(delete_map_actual))
		t.Fatalf("delete could not be diffed")
	}

	if !reflect.DeepEqual(modify_map_reference, modify_map_actual) {
		t.Logf(" modify_handler reference %d actual  %d", len(modify_map_reference), len(modify_map_actual))

		t.Logf("refer %+v", modify_map_reference)
		t.Logf("refer %+v", modify_map_actual)
		t.Fatalf("modify could not be diffed")
	}

	// now we must swap the trees

	insert_map_actual = map[string]string{}
	delete_map_actual = map[string]string{}
	modify_map_actual = map[string]string{}

	insert_map_reference, delete_map_reference = delete_map_reference, insert_map_reference
	for k, _ := range modify_map_reference {
		modify_map_reference[k] = keyval_reference[k]
	}

	base_tree, head_tree = head_tree, base_tree // swap the actual tree

	err = Diff(base_tree, head_tree, delete_handler, modify_handler, insert_handler)

	//t.Logf("err  %s",err)

	//t.Logf("delete %d modify %d insert %d", len(delete_map_actual), len(modify_map_actual),  len(insert_map_actual)  )

	if !reflect.DeepEqual(insert_map_reference, insert_map_actual) {
		t.Logf(" insert map reference %d actual  %d", len(insert_map_reference), len(insert_map_actual))
		t.Fatalf("inserts could not be diffed")
	}

	if !reflect.DeepEqual(delete_map_reference, delete_map_actual) {
		t.Logf(" delete map count %d", len(delete_map_actual))
		t.Fatalf("delete could not be diffed")
	}

	if !reflect.DeepEqual(modify_map_reference, modify_map_actual) {
		t.Logf(" modify_handler map count %d ", len(modify_map_actual))
		t.Fatalf("modify could not be diffed")
	}

}

// this test various cursor diff related code
func TestDiffTree_Single_Key(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	rand.Seed(time.Now().UnixNano())

	key := randString(32)
	value := randString(8)
	mod_value := randString(8)

	tree.Put([]byte(key), []byte(value))

	tree.Commit() // commit the tree
	//tree.Hash()

	//tree.graph("/tmp/1.dot")

	tree.Put([]byte(key), []byte(mod_value))
	tree.Commit() // commit the tree
	// tree.graph("/tmp/2.dot")

	gv1, _ := store.LoadSnapshot(1)
	base_tree, _ := gv1.GetTree("root")
	//base_tree.graph("/tmp/11.dot")

	gv2, _ := store.LoadSnapshot(2)
	head_tree, _ := gv2.GetTree("root")
	// head_tree.graph("/tmp/12.dot")

	insert_handler := func(k, v []byte) {
		panic("insert")
	}
	delete_handler := func(k, v []byte) {
		panic("insert")
	}

	mod_count := 0

	modify_handler := func(k, v []byte) {
		mod_count++
		if mod_count > 1 {
			panic("more modified")
		}
		if key != string(k) || mod_value != string(v) {
			panic("modify failed")
		}
	}

	err = Diff(base_tree, head_tree, delete_handler, modify_handler, insert_handler)

	mod_count = 0

	modify_handler = func(k, v []byte) {

		//t.Logf("value %s mod_value %s v %s" , value, mod_value, string(v))
		mod_count++
		if mod_count > 1 {
			panic("more modified")
		}
		if key != string(k) || value != string(v) {
			panic("modify failed")
		}
	}

	err = Diff(head_tree, base_tree, delete_handler, modify_handler, insert_handler)

}

// this test various diff related codes
// tree needs to be a particuar case, as some paths are hardcoded and manually triggered
func TestDiffTree_Single_Key_error_cases(t *testing.T) {
	store, err := NewMemStore()
	require.NoError(t, err)
	gv, err := store.LoadSnapshot(0)
	require.NoError(t, err)

	tree, err := gv.GetTree("root")
	require.NoError(t, err)

	rand.Seed(1000)

	key := randString(32)
	value := randString(8)

	tree.Put([]byte(key), []byte(value))

	tree.Commit() // commit the tree

	for i := 0; i < 8; i++ {
		key1 := randStr(60)
		value1 := randStr(512)
		tree.Put([]byte(key1), []byte(value1))
	}
	tree.Commit() // commit the tree

	gv1, _ := store.LoadSnapshot(1)
	base_tree, _ := gv1.GetTree("root")
	//base_tree.graph("/tmp/11.dot")

	gv2, _ := store.LoadSnapshot(2)
	head_tree, _ := gv2.GetTree("root")
	// let corrupt head tree

	head_tree.root.findex = 1000000000
	head_tree.root.loaded_partial = true

	dt := diffTree{base_tree: base_tree, head_tree: head_tree}
	require.Error(t, dt.compare_nodes(base_tree.root, head_tree.root, nil, nil, nil))

	base_tree, _ = gv1.GetTree("root")
	base_tree.root.findex = 1000000000
	base_tree.root.loaded_partial = true
	require.Error(t, dt.compare_nodes(base_tree.root, head_tree.root, nil, nil, nil))

	base_tree, _ = gv1.GetTree("root")
	head_tree, _ = gv2.GetTree("root")

	head_tree.root.left.(*inner).findex = 1000000000
	head_tree.root.left.(*inner).loaded_partial = true
	require.Error(t, dt.compare_nodes(nil, head_tree.root, nil, nil, nil))

	//	t.Logf("basehash %x", base_tree.Hash())
	//	t.Logf("headhash %x", head_tree.Hash())

	require.Error(t, Diff(base_tree, head_tree, nil, nil, nil))

}
