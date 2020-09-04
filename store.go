package graviton

import "io"
import "os"
import "fmt"
import "path/filepath"
import "sync"

import "encoding/binary"

import "golang.org/x/xerrors"

// all file operations will go through this
// If this is implemented through an interface, it will trigger memory allocations on heap
// this crude implementation serves the purpose and also allows to implement arbitary storage backends
type file struct {
	diskfile   *os.File // used for disk backend
	memoryfile []byte   // used for memory backend
	size       uint32
}

type storage_layer_type int8

const (
	unknown_layer storage_layer_type = iota // default is unknown layer
	disk
	memory
)

// Store is the backend which is used to store the data in serialized form to disk.
// The data is stored in files in split format and total number of files can be 4 billion.
// each file is upto 2 GB in size, this limit has been placed to support FAT32 which restricts files to 4GB
type Store struct {
	storage_layer storage_layer_type // identify storage layer

	base_directory string

	files  map[uint32]*file
	findex uint32

	versionrootfile *file // only maintains recent version records

	version_index       int       // version index to rotate inside version data
	version_data        [512]byte // stores version data pointers, 20 * 24 , each record is 24 bytes
	version_data_loaded bool      // whether the version data loaded

	//internal_value_root *inner // internal append only value root
	commitsync sync.RWMutex // used to sync altroots value root, versioned root
	discsync   sync.Mutex   // used to syncronise disc swrites
}

// start a  new memory backed store which may be useful for testing and other temporaray use cases.
func NewMemStore() (*Store, error) {
	s := &Store{storage_layer: memory, files: map[uint32]*file{}}
	return s.init()
}

// open/create a disk based store, if the directory pre-exists, it is used as is. Since we are an append only keyvalue
// store, we do not delete any data.
func NewDiskStore(basepath string) (*Store, error) {
	if err := os.MkdirAll(basepath, 0700); err != nil {
		return nil, fmt.Errorf("direction creation err %s  dirpath %s \n", err, basepath)
	}
	s := &Store{storage_layer: disk, base_directory: basepath, files: map[uint32]*file{}}
	return s.init()
}

func (store *Store) Close() {

	switch store.storage_layer {
	case disk:
		for _, f := range store.files {
			f.diskfile.Close()
		}
		store.versionrootfile.diskfile.Close()

	case memory:
		for _, f := range store.files {
			f.memoryfile = nil
		}
	default:
		panic("unknown storage layer")
	}

}

// init and load some items from the store
func (s *Store) init() (*Store, error) {
	return s, s.loadfiles()
}

// 4 billion files  each of 4 GB seems to be enough for quite some time, we will run out of handles much earlier
// note that the structure is independant of these pointers and can thus be extended at any point in time in future
func (s *Store) uint_to_filename(n uint32) string {
	switch s.storage_layer {
	case disk:
		d, c, b, a := n>>24, ((n >> 16) & 0xff), ((n >> 8) & 0xff), n
		return filepath.Join(s.base_directory, fmt.Sprintf("%d", d), fmt.Sprintf("%d", c), fmt.Sprintf("%d", b), fmt.Sprintf("%d", a)+".dfs")

	case memory:
		fallthrough
	default:
		panic("unknown storage layer")
	}

}

// load all files from the disk
// we may need to increase file handles
func (s *Store) loadfiles() error {

	if s.storage_layer == disk {
		if file_handle, err := os.OpenFile(filepath.Join(s.base_directory, "version_root.bin"), os.O_CREATE|os.O_RDWR, 0600); err != nil {
			return xerrors.Errorf("%w:  index %d, filename %s", err, s.findex, s.uint_to_filename(uint32(s.findex)))
		} else {
			s.versionrootfile = &file{diskfile: file_handle}
		}
	} else if s.storage_layer == memory {
		s.versionrootfile = &file{memoryfile: []byte{}, size: uint32(0)}
	} else {
		return fmt.Errorf("unknown storage layer")
	}

	for i := uint32(0); i < (4*1024*1024*1024)-1; i++ {

		if s.storage_layer == disk {

			filename := s.uint_to_filename(uint32(i))

			finfo, err := os.Stat(filename)
			if os.IsNotExist(err) { // path/to/whatever does not exist
				break
			}

			if finfo != nil && finfo.IsDir() {
				return fmt.Errorf("expected file but found directory at path %s", filename)
			}

			file_handle, err := os.OpenFile(filename, os.O_RDWR, 0600)
			if err != nil {
				return fmt.Errorf("%s: filename:%s", err, filename)
			}

			s.files[i] = &file{diskfile: file_handle, size: uint32(finfo.Size())}
			s.findex = i

		} else if s.storage_layer == memory {
			// nothing to do memory always starts afresh
			break
		}

	}

	if len(s.files) == 0 {
		return s.create_first_file()
	}

	return nil
}

func (s *Store) create_first_file() error {

	if s.storage_layer == disk {

		err := os.MkdirAll(filepath.Dir(s.uint_to_filename(0)), 0700)
		if err != nil {
			return fmt.Errorf("direction creation err %s  filename %s \n", err, s.uint_to_filename(0))
		}
		if file_handle, err := os.OpenFile(s.uint_to_filename(0), os.O_CREATE|os.O_RDWR, 0600); err != nil {
			return xerrors.Errorf("%w:  index %d, filename %s", err, 0, s.uint_to_filename(uint32(0)))
		} else {

			file_handle.Write([]byte{0x0}) // write a byte so as mark 0,0 as invalid
			s.findex = 0
			s.files[s.findex] = &file{diskfile: file_handle, size: uint32(1)}
		}
	} else if s.storage_layer == memory {
		s.findex = 0
		s.files[s.findex] = &file{memoryfile: []byte{1}, size: uint32(1)} // write a byte so as mark 0,0 as invalid
	}
	return nil
}

// we are here means we have a currently open file
// this function is single threaded
func (s *Store) write(buf []byte) (uint32, uint32, error) {
	var done int
	var err error

	s.discsync.Lock()
	//defer s.discsync.Unlock() // defer has been removed to removed overhead

	cfile, ok := s.files[s.findex]

	if !ok || len(s.files) < 1 {
		s.discsync.Unlock()
		return 0, 0, fmt.Errorf("invalid file structures")
	}

	// // check whether we need to open a new file or overflowing
	if cfile.size+uint32(len(buf)) > MAX_FILE_SIZE || cfile.size+uint32(len(buf)) < cfile.size {
		s.findex++

		if s.storage_layer == disk {
			err := os.MkdirAll(filepath.Dir(s.uint_to_filename(uint32(s.findex))), 0700)
			if err != nil {
				s.discsync.Unlock()
				return 0, 0, fmt.Errorf("direction creation err %s  filename %s \n", err, s.uint_to_filename(s.findex))
			}
			if file_handle, err := os.OpenFile(s.uint_to_filename(uint32(s.findex)), os.O_CREATE|os.O_RDWR, 0600); err != nil {
				s.discsync.Unlock()
				return 0, 0, xerrors.Errorf("%w:  index %d, filename %s", err, s.findex, s.uint_to_filename(uint32(s.findex)))
			} else {
				s.files[s.findex] = &file{diskfile: file_handle}
				cfile = s.files[s.findex]
			}
		} else if s.storage_layer == memory {
			s.files[s.findex] = &file{memoryfile: []byte{}, size: uint32(0)} // write a byte so as mark 0,0 as invalid
			cfile = s.files[s.findex]
		} else {
			return 0, 0, fmt.Errorf("unknown storage layer")
		}

	}

	pos := cfile.size

	if s.storage_layer == disk {
		done, err = cfile.diskfile.WriteAt(buf, int64(cfile.size))
	} else if s.storage_layer == memory {

		if int64(len(cfile.memoryfile)) != int64(cfile.size) {
			//	fmt.Printf("filesize %d , len of memory %d\n", cfile.size,len(cfile.memoryfile))
			return 0, 0, fmt.Errorf("probable store is closed")
		}
		cfile.memoryfile = append(cfile.memoryfile, buf...)
		done += len(buf)
	}

	cfile.size += uint32(done)
	s.discsync.Unlock()
	return s.findex, pos, err

}

func (s *Store) read(findex, fpos uint32, buf []byte) (int, error) {
	if cfile, ok := s.files[findex]; !ok {
		return 0, fmt.Errorf("findex not available") //xerrors.Errorf("data file (indexed at %d) is NOT available", findex)
	} else {

		if s.storage_layer == disk {
			c, err := cfile.diskfile.ReadAt(buf, int64(fpos))
			return c, err

		} else if s.storage_layer == memory {

			if fpos < uint32(len(cfile.memoryfile)) {
				c := copy(buf, cfile.memoryfile[fpos:])
				return c, nil
			} else if fpos == uint32(len(cfile.memoryfile)) {
				return 0, io.EOF
			} else {
				return 0, fmt.Errorf("out of range")
			}

		} else {
			return 0, fmt.Errorf("unknown storage layer")
		}
	}

}

func (s *Store) writeVersionData(version uint64, findex, fpos uint32) error {
	var buf [512]byte

	s.discsync.Lock()
	defer s.discsync.Unlock()

	copy(buf[:], s.version_data[:])
	index := (s.version_index + 1) % internal_MAX_VERSIONS_TO_KEEP
	binary.LittleEndian.PutUint64(buf[index*internal_VERSION_RECORD_SIZE+0:], version)
	binary.LittleEndian.PutUint64(buf[index*internal_VERSION_RECORD_SIZE+8:], uint64(findex))
	binary.LittleEndian.PutUint64(buf[index*internal_VERSION_RECORD_SIZE+16:], uint64(fpos))

	if s.storage_layer == disk {
		if _, err := s.versionrootfile.diskfile.WriteAt(buf[:], 0); err != nil {
			return err
		}
	} else if s.storage_layer == memory {
		s.versionrootfile.memoryfile = append(s.versionrootfile.memoryfile[:0], buf[:]...)
	} else {
		return fmt.Errorf("unknown storage layer")
	}

	copy(s.version_data[:], buf[:])
	s.version_index = index
	return nil

}

// load recent snapshot list to ram
func (s *Store) loadsnapshottablestoram() (err error) {
	var buf [512]byte

	if s.storage_layer == disk {
		if finfo, err := s.versionrootfile.diskfile.Stat(); err == nil { // if newly created file, it return 0
			if finfo.Size() == 0 {
				copy(s.version_data[:], buf[:])
				s.version_data_loaded = true
				return nil
			}
		} else {
			return err
		}
	} else if s.storage_layer == memory {
		if len(s.versionrootfile.memoryfile) == 0 {
			copy(s.version_data[:], buf[:])
			s.version_data_loaded = true
			return nil
		}
	} else {
		return fmt.Errorf("unknown storage layer")
	}

	var bytes_count int

	if s.storage_layer == disk {
		bytes_count, err = s.versionrootfile.diskfile.ReadAt(buf[:], 0)
	} else if s.storage_layer == memory {
		bytes_count = copy(buf[:], s.versionrootfile.memoryfile)
	}

	if bytes_count == 512 {
		copy(s.version_data[:], buf[:])
		s.version_data_loaded = true
		s.version_index, _, _, _ = s.findhighestsnapshotinram() // setup index properly
		return nil
	}

	return err
}
