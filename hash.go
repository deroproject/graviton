package graviton

import "hash"

//import "crypto/sha256"
import "golang.org/x/crypto/blake2s"

const lastBit = HASHSIZE*8 - 1

var zerosHash, zeros [HASHSIZE]byte // all empty nodes have this hash

func hasher() hash.Hash {
	//return sha256.New()
	h, _ := blake2s.New256(nil)
	return h
}

func sum(key []byte) (keyhash [HASHSIZE]byte) {
	return blake2s.Sum256(key)
}

func init() {
	h := hasher()
	h.Write([]byte{leafNODE})
	h.Write(zeros[:])
	tmp := zerosHash[:0]
	h.Sum(tmp)
}
