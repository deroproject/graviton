package graviton

const (
	nullNODE byte = iota
	innerNODE
	leafNODE
)

// we can get away with runtime type detection
func getNodeType(n node) byte {
	switch n.(type) {
	case nil:
		return nullNODE
	case *inner:
		return innerNODE
	case *leaf:
		return leafNODE
	default:
		panic("unknown type")
	}
}

// TODO  simplify node
type node interface {
	isDirty() bool
	load_partial(*Store) error
	Hash(*Store) ([]byte, error)
	Get(*Store, [HASHSIZE]byte) ([]byte, error)
	Delete(*Store, [HASHSIZE]byte) (bool, bool, error)
	Position() (uint32, uint32)
	Prove(*Store, [HASHSIZE]byte, *Proof) error
}

// these will enable processing of all bits collective from MSB to LSB
func setBit(keyhash []byte, index uint) {
	pos, bit := index/8, index%8
	keyhash[pos] = (keyhash[pos] | (1 << (8 - (bit + 1))))
}

func isBitSet(keyhash []byte, index uint) bool {
	pos, bit := index/8, index%8
	return (keyhash[pos] & (1 << (8 - (bit + 1)))) > 0
}
