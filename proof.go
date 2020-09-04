package graviton

import "fmt"
import "bytes"
import "encoding/binary"

const (
	member byte = iota + 1
	collision
	deadend
)

func NewProof() *Proof {
	return &Proof{
		trace: make([][]byte, 0, HASHSIZE*32),
	}
}

//This structure is used to prove existence/non-existence of a key
type Proof struct {
	version int64
	ptype   byte
	trace   [][]byte
	value   []byte // can this be a problem, maybe we should decouple value and replace it with hash of value,

	ckey, cval []byte // collision key and value
}

// prepare the structure for reuse
func (p *Proof) Reset() {
	p.version = 0
	for i := range p.trace {
		p.trace[i] = nil
	}
	p.value = nil
	p.ckey = nil
	p.cval = nil
	p.ptype = 0
	p.trace = p.trace[:0]
}

// add paths
func (p *Proof) addTrace(hash []byte) {
	p.trace = append(p.trace, hash)
}

func (p *Proof) addDeadend() {
	p.ptype = deadend
}

func (p *Proof) addValue(value []byte) {
	p.ptype = member
	p.value = value
}

func (p *Proof) addCollision(key, val []byte) {
	p.ptype = collision
	p.ckey = key
	p.cval = val
}

func (p *Proof) rootForLeaf(keyhash [HASHSIZE]byte, leaf []byte) []byte {
	h := hasher()
	rst := make([]byte, HASHSIZE)
	copy(rst, leaf)
	last := len(p.trace) - 1
	for i := uint(last); ; i-- {
		sibling := p.trace[i]
		h.Write([]byte{innerNODE})
		if isBitSet(keyhash[:], i) {
			h.Write(sibling)
			h.Write(rst)
		} else {
			h.Write(rst)
			h.Write(sibling)
		}
		rst = h.Sum(rst[:0])
		h.Reset()
		if i == 0 {
			break
		}
	}
	return rst
}

func (p *Proof) VerifyMembership(root [HASHSIZE]byte, key []byte) bool {
	return p.verifyMembershipRaw(root, sum(key))
}

func (p *Proof) verifyMembershipRaw(root [HASHSIZE]byte, key [HASHSIZE]byte) bool {
	rst := sum(p.value)
	return bytes.Compare(root[:], p.rootForLeaf(key, leafHash(key[:], rst[:]))) == 0
}

func (p *Proof) VerifyNonMembership(root [HASHSIZE]byte, key []byte) bool {
	return p.verifyNonMembershipRaw(root, sum(key))
}

func (p *Proof) verifyNonMembershipRaw(root [HASHSIZE]byte, key [HASHSIZE]byte) bool {
	if p.ptype == collision {
		return bytes.Compare(root[:], p.rootForLeaf(key, leafHash(p.ckey, p.cval))) == 0
	}
	if p.ptype == deadend {
		return bytes.Compare(root[:], p.rootForLeaf(key, zerosHash[:])) == 0
	}
	return false
}

// if the proof is for existence for a key, it's associated value can be read here
func (p *Proof) Value() []byte {
	if p.value == nil {
		return []byte{}
	}
	rst := make([]byte, len(p.value))
	copy(rst, p.value)
	return rst
}

// Serialize the proof to a byte array
func (p *Proof) Marshal() []byte {
	var b bytes.Buffer
	p.MarshalTo(&b)
	return b.Bytes()
}

// Serialize the proof to a bytes Buffer
// 		the following are the size requirements for proof
// 		1 byte for version
// 		1 byte for type
// 		varint trace length
// 		32 byte(HASHSIZE) tracebits, bit 1 is set  if hash is not zerohash
// 		32 byte(HASHSIZE) * number of trace bits set
// 		if collision is there 32 byte(HASHSIZE) key , 32 byte(HASHSIZE) value
// 		if member, varint lenth var int length prefixed value
// 		dead end = 0
func (p *Proof) MarshalTo(b *bytes.Buffer) {

	var buf [10]byte
	var emptyhash [HASHSIZE]byte // used for proof bits
	b.WriteByte(1)               // write version
	b.WriteByte(p.ptype)         // write proof type

	done := binary.PutUvarint(buf[:], uint64(len(p.trace)))
	b.Write(buf[:done])

	tracebits_pos := 2 + done
	b.Write(emptyhash[:])

	tracebits := b.Bytes()
	tracebits = tracebits[tracebits_pos:] // can buffer relocation cause any issues

	for i := range p.trace {
		if bytes.Compare(p.trace[i], zerosHash[:]) != 0 {
			setBit(tracebits, uint(i))
			b.Write(p.trace[i])
		}
	}
	switch p.ptype {
	case collision:
		b.Write(p.ckey) // HASHSIZE len
		b.Write(p.cval) // HASHSIZE len
	case member:
		done := binary.PutUvarint(buf[:], uint64(len(p.value)))
		b.Write(buf[:done])
		b.Write(p.value[:]) // why don't we write value hash and dispatch value separately,
	case deadend:
	}

	bb := b.Bytes()
	copy(bb[tracebits_pos:], tracebits)
}

// Unmarshal follows reverse of marshal to deserialize the array of bytes to proof for verification.
func (p *Proof) Unmarshal(buf []byte) error {

	p.Reset() // reset complete proof

	p.version = int64(buf[0]) // note we are currently not checking versio
	p.ptype = buf[1]
	tracelength, tracelengthsize := binary.Uvarint(buf[2:])
	if tracelengthsize <= 0 || tracelength < 1 {
		return fmt.Errorf("invalid proof tracelength")
	}
	p.trace = make([][]byte, tracelength)
	done := 1 + 1 + int(tracelengthsize)
	tracebits := buf[done : done+HASHSIZE]

	done += HASHSIZE
	for i := range p.trace {
		if isBitSet(tracebits, uint(i)) {
			p.trace[i] = make([]byte, HASHSIZE)
			copy(p.trace[i], buf[done:]) // copy only HASHSIZE
			done += HASHSIZE
		} else {
			p.trace[i] = zerosHash[:] // if any bit is not set, use zerohash
		}
	}
	switch p.ptype {
	case collision:
		p.ckey = make([]byte, HASHSIZE)
		copy(p.ckey, buf[done:])
		done += HASHSIZE
		p.cval = make([]byte, HASHSIZE)
		copy(p.cval, buf[done:])
	case member:
		valuelength, valuelengthsize := binary.Uvarint(buf[done:])

		if valuelengthsize <= 0 {
			return fmt.Errorf("invalid proof")
		}
		done += int(valuelengthsize)
		p.value = make([]byte, valuelength)
		copy(p.value, buf[done:])

	}
	return nil
}
