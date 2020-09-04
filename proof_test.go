package graviton

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTreeProve(t *testing.T) {
	rand.Seed(100)
	_, tree := setupDeterministicTree(t, 10)

	key := make([]byte, 20)
	value := make([]byte, 10)
	rand.Read(key)
	rand.Read(value)
	require.NoError(t, tree.Put(key, value))

	proof, err := tree.GenerateProof(key)
	require.NoError(t, err)
	require.True(t, proof.VerifyMembership(tree.hashSkipError(), key))

	mproof := proof.Marshal()
	var decoded Proof

	decoded.Unmarshal(mproof)

	require.True(t, decoded.VerifyMembership(tree.hashSkipError(), key))
	require.False(t, decoded.VerifyNonMembership(tree.hashSkipError(), key))

	// value is last 11 bytes

	copy(mproof[len(mproof)-11:], []byte{0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88})
	require.Error(t, decoded.Unmarshal(mproof))

	// test malformed proofs, small buffer give array indexing array
	wrong_proof := []byte{1, collision, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88}
	require.Error(t, decoded.Unmarshal(wrong_proof))

}

func TestTreeProvePersistent(t *testing.T) {
	_, tree := setupDeterministicTree(t, 4)

	var key = make([]byte, 10)
	rand.Read(key)
	require.NoError(t, tree.Put(key, key))

	root := tree.hashSkipError()
	require.NoError(t, tree.Commit())

	proof, err := tree.GenerateProof(key)
	require.NoError(t, err)
	require.True(t, proof.VerifyMembership(tree.hashSkipError(), key))

	require.Equal(t, key, proof.Value())
	require.True(t, proof.VerifyMembership(root, key))

	proof.Reset()

	if len(proof.Value()) != 0 {
		t.Fatalf("Proof not successfully reset.")
	}
}

// these cannot be generated using exported api
func TestProveCollision(t *testing.T) {
	_, tree := setupDeterministicTree(t, 0)
	value := []byte("testcollision")

	for i := 0; i < 8; i++ {
		key := [HASHSIZE]byte{}
		key[0] = 1 << uint(i)
		require.NoError(t, tree.putRaw(key, nil, value))
	}

	key := [HASHSIZE]byte{}
	key[0] = 0xf0 // this needs to be flipped if if bit prrocessing is switched
	require.NoError(t, tree.putRaw(key, nil, value))
	root := tree.hashSkipError()

	//require.NoError(t, tree.Commit())

	ckey := [HASHSIZE]byte{}
	ckey[0] = 0xf8 // this needs to be flipped if if bit prrocessing is switched
	proof := NewProof()
	require.NoError(t, tree.generateProofRaw(ckey, proof))

	require.False(t, proof.verifyMembershipRaw(root, key))

	require.True(t, proof.verifyNonMembershipRaw(root, key))

}

func TestProveDeadend(t *testing.T) {
	_, tree := setupDeterministicTree(t, 0)
	value := []byte("testdeadend")

	gET_CHECKED = false // disable error checking of values

	defer func() {
		gET_CHECKED = true
	}()

	order := []byte{
		0x0,  //00000000,
		0xc0, //11000000,
		0xd0, //11010000,
		0x8,  //10000000,
	}
	for i := range order {
		key := [HASHSIZE]byte{}
		key[0] = order[i]
		require.NoError(t, tree.putRaw(key, nil, value))
	}

	key := [HASHSIZE]byte{}
	key[0] = 0xE0 //11100000
	root := tree.hashSkipError()

	require.NoError(t, tree.Commit())

	proof := NewProof()
	require.NoError(t, tree.generateProofRaw(key, proof))

	require.False(t, proof.verifyMembershipRaw(root, key))
	require.True(t, proof.verifyNonMembershipRaw(root, key))

	{
		order := []byte{
			0x0,  //00000000,
			0x20, //11000000,
			0xd0, //11010000,
			0x8,  //10000000,
		}
		for i := range order {
			key := [HASHSIZE]byte{}
			key[0] = order[i]
			require.NoError(t, tree.putRaw(key, nil, value))
		}

		key := [HASHSIZE]byte{}
		key[0] = 0xE0 //11100000
		//root := tree.Hash()

		require.NoError(t, tree.Commit())

	}

}

func TestProofMarshal(t *testing.T) {
	_, tree := setupDeterministicTree(t, 1000)
	root := tree.hashSkipError()

	key := make([]byte, 20)
	for i := 0; i < 10; i++ {
		rand.Read(key)
		proof, err := tree.GenerateProof(key[:])
		require.NoError(t, err)

		member := proof.VerifyMembership(root, key)
		non := proof.VerifyNonMembership(root, key)

		buf := proof.Marshal()

		marshalled := NewProof()
		marshalled.Unmarshal(buf)

		require.Equal(t, proof, marshalled)

		require.Equal(t, member, marshalled.VerifyMembership(root, key))
		require.Equal(t, non, marshalled.VerifyNonMembership(root, key))

		proof.Reset()
	}
}

func TestProveAfterDelete(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	_, tree := setupDeterministicTree(t, 0)
	proof := NewProof()

	key1 := make([]byte, 20)
	rand.Read(key1)

	require.NoError(t, tree.Put(key1, key1))
	keys := [][]byte{}
	for i := 0; i < 7; i++ {
		key2 := make([]byte, 20)
		rand.Read(key2)
		require.NoError(t, tree.Put(key2, key2))
		keys = append(keys, key2)
	}

	proof, err := tree.GenerateProof(key1[:])
	require.NoError(t, err)
	require.True(t, proof.VerifyMembership(tree.hashSkipError(), key1))

	for _, key := range keys {
		require.NoError(t, tree.Delete(key))
	}
	proof.Reset()
	proof, err = tree.GenerateProof(key1[:])
	require.NoError(t, err)
	require.True(t, proof.VerifyMembership(tree.hashSkipError(), key1))
}
