package graviton

import "errors"

const (
	HASHSIZE_BYTES  = 32 // we currently are using blake hash which is 256 bits or 32 bytes
	HASHSIZE        = HASHSIZE_BYTES
	HASHSIZE_BITS   = HASHSIZE_BYTES * 8     // hash size in bits
	MINBLOCK        = 512                    // max block size excluding value
	MAX_KEYSIZE     = MINBLOCK - 64          // 64 bytes are reserved, keys are limited to 448 bytes
	MAX_FILE_SIZE   = 2 * 1024 * 1024 * 1024 // 2GB since we use split files to store data chunks
	MAX_VALUE_SIZE  = 100 * 1024 * 1024      // values are limited to this size
	TREE_NAME_LIMIT = 127                    // TREE name cannot be larger than this in bytes ( not in utf8 chars)
)

const internal_MAX_VERSIONS_TO_KEEP = 20 // this many recent versions will be kept
const internal_VERSION_RECORD_SIZE = 24  // three uint64

var (
	ErrNotFound         = errors.New("leaf not found")
	ErrVersionNotStored = errors.New("no such version")
	ErrCorruption       = errors.New("Data Corruption")
	ErrNoMoreKeys       = errors.New("No more keys exist")
)
