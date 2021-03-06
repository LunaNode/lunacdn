package lunacdn

/*
cache_obj.go: caches blocks on disk and in memory, and serves to peers and downloaders
On cache miss, we try to serve the block from a peer via PeerList instance.
*/

import "sync"
import "time"
import "fmt"
import "io"
import "io/ioutil"
import "os"
import "strconv"
import "runtime"
import "strings"
import "errors"
import "path/filepath"

/*
Synchronization strategy:
  - ObjFile are not modified once registered
  - Lock on the block to modify OnDisk/Data
  - Always acquire global lock first, then block lock
*/

type ObjBlock struct {
	mu sync.Mutex
	File *ObjFile
	Index int
	Offset int64
	OnDisk bool

	// cached contents, or nil
	Data []byte
}

/*
 * Files that are registered with the cache.
 * We keep these objects permanently in memory, so the files may not actually be available.
 * ObjFile must not be modified once added to the Cache.files map
 */
type ObjFile struct {
	PathHash string
	Length int64
	NumBlocks uint16
	BlockSize uint32
	Blocks []*ObjBlock
}

// implement CacheFile interface
func (this *ObjFile) GetPathHash() string {
	return this.PathHash
}
func (this *ObjFile) GetLength() int64 {
	return this.Length
}
func (this *ObjFile) GetNumBlocks() uint16 {
	return this.NumBlocks
}
func (this *ObjFile) GetBlockSize() uint32 {
	return this.BlockSize
}

type ObjCache struct {
	mu sync.Mutex

	// stored files, map from hashed file path to file object
	files map[string]*ObjFile

	// blocks currently stored in-memory or on-disk
	cachedInMemory map[*ObjBlock]time.Time
	cachedOnDisk map[*ObjBlock]time.Time

	// how many uncached from cachedInMemory before last runtime.GC() call
	uncachesSinceFree int

	memoryLimit int
	diskLimit int
	cacheLocation string
	peerList *PeerList
}

func MakeObjCache(cfg *Config, peerList *PeerList) *ObjCache {
	this := new(ObjCache)
	this.files = make(map[string]*ObjFile)
	this.peerList = peerList
	this.cachedInMemory = make(map[*ObjBlock]time.Time)
	this.cachedOnDisk = make(map[*ObjBlock]time.Time)

	this.cacheLocation = cfg.CachePath
	this.memoryLimit = cfg.CacheMemory * 1024 * 1024 / BLOCK_SIZE
	if !cfg.ModeNoDelete {
		this.diskLimit = cfg.CacheDisk * 1024 * 1024 * 1024 / BLOCK_SIZE
	} else {
		this.diskLimit = -1
	}

	this.Load()
	return this
}

func (this *ObjCache) blockPath(block *ObjBlock) string {
	blockSubdir := hexHash(fmt.Sprintf("%s%d", block.File.PathHash, block.Index))
	return filepath.Join(this.cacheLocation, "obj", blockSubdir[:2], fmt.Sprintf("%s_%d.obj", block.File.PathHash, block.Index))
}

func (this *ObjCache) NotifyFile(hash string, length int64, numBlocks uint16, blockSize uint32) {
	this.mu.Lock()
	defer this.mu.Unlock()

	// register new file only if not already registered
	_, ok := this.files[hash]
	if ok {
		return
	}

	file := this.appendFile(hash, length, numBlocks, blockSize)

	// create a .meta file so we can retrieve the CacheFile data on restart
	metaString := fmt.Sprintf("%s:%d:%d:%d", file.PathHash, file.Length, file.NumBlocks, file.BlockSize)
	metaFile := filepath.Join(this.cacheLocation, "meta", file.PathHash[0:2], fmt.Sprintf("%s.meta", file.PathHash))
	err := ioutil.WriteFile(metaFile, []byte(metaString), 0644)
	if err != nil {
		Log.Warn.Printf("Failed to write file metadata to [%s]: %s", metaFile, err.Error())
	}
}

func (this *ObjCache) appendFile(hash string, length int64, numBlocks uint16, blockSize uint32) *ObjFile {
	// warn if BLOCK_SIZE seems to have mismatch
	expectedNumBlocks := uint16((length + int64(blockSize) - 1) / int64(blockSize)) // number blocks needed to store the length with blocks of size BLOCK_SIZE
	if numBlocks != expectedNumBlocks {
		Log.Error.Printf("Mismatched numBlocks for file %s (%d, expected %d)", hash, numBlocks, expectedNumBlocks)
	} else if blockSize != BLOCK_SIZE {
		Log.Warn.Printf("File %s has different number of blocks than local BLOCK_SIZE parameter (file: %d, local: %d)", hash, blockSize, BLOCK_SIZE)
	}

	// add the file
	file := &ObjFile{PathHash: hash, Length: length, NumBlocks: numBlocks, BlockSize: blockSize}
	for i := 0; i < int(numBlocks); i++ {
		block := ObjBlock{File: file, Index: i, Offset: int64(i) * BLOCK_SIZE, OnDisk: false}
		file.Blocks = append(file.Blocks, &block)
	}
	this.files[hash] = file
	return file
}

func (this *ObjCache) DownloadInit(path string) (CacheFile, error) {
	return this.DownloadInitHash(hexHash(path))
}

func (this *ObjCache) DownloadInitHash(pathHash string) (CacheFile, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	file := this.files[pathHash]

	if file == nil {
		return file, errors.New("file not found")
	} else {
		return file, nil
	}
}

func (this *ObjCache) DownloadRead(cacheFile CacheFile, index int, tryPeers bool) []byte {
	file, ok := cacheFile.(*ObjFile)
	if !ok {
		Log.Error.Printf("DownloadRead got bad request, cacheFile is not ObjFile type")
		return nil
	}

	if index >= len(file.Blocks) {
		return nil
	}

	block := file.Blocks[index]
	block.mu.Lock()
	defer block.mu.Unlock()

	if block.Data != nil {
		go this.accessedBlock(block) // update access time, clean cache
		return block.Data
	}

	if block.OnDisk {
		blockBytes, err := ioutil.ReadFile(this.blockPath(block))

		if err != nil {
			Log.Error.Printf("Failed to read block from filesystem: %s", err.Error())
			return nil
		} else {
			block.Data = blockBytes
			go this.accessedBlock(block) // update access time, clean cache
			return blockBytes
		}
	}

	// download from peers
	if tryPeers {
		bytes := this.peerList.DownloadBlock(file.PathHash, index)

		if bytes != nil {
			block.Data = bytes
			err := ioutil.WriteFile(this.blockPath(block), bytes, 0644)
			if err != nil {
				Log.Warn.Printf("Failed to save block to filesystem: %s", err.Error())
			} else {
				block.OnDisk = true
			}
			go this.accessedBlock(block) // update access time, clean cache
			return bytes
		}
	}

	return nil
}

// expects caller to have global lock
// we will not lock any other mutexes within this function
func (this *ObjCache) accessedBlock(updateBlock *ObjBlock) {
	this.mu.Lock()
	defer this.mu.Unlock()

	// first update cache for the block
	updateBlock.mu.Lock()
	if updateBlock.Data != nil {
		this.cachedOnDisk[updateBlock] = time.Now()
		this.cachedInMemory[updateBlock] = time.Now()
	} else if updateBlock.OnDisk {
		this.cachedOnDisk[updateBlock] = time.Now()
	}
	updateBlock.mu.Unlock()

	for len(this.cachedInMemory) > this.memoryLimit {
		var lruBlock *ObjBlock

		for block, time := range this.cachedInMemory {
			if lruBlock == nil || this.cachedInMemory[lruBlock].After(time) {
				lruBlock = block
			}
		}

		lruBlock.mu.Lock()
		lruBlock.Data = nil
		delete(this.cachedInMemory, lruBlock)
		this.uncachesSinceFree++
		lruBlock.mu.Unlock()
	}

	if this.uncachesSinceFree >= this.memoryLimit / 2 {
		runtime.GC()
		this.uncachesSinceFree = 0
	}

	for this.diskLimit != -1 && len(this.cachedOnDisk) > this.diskLimit {
		var lruBlock *ObjBlock

		for block, time := range this.cachedOnDisk {
			if lruBlock == nil || this.cachedOnDisk[lruBlock].After(time) {
				lruBlock = block
			}
		}

		lruBlock.mu.Lock()
		if lruBlock.Data != nil {
			lruBlock.Data = nil
			delete(this.cachedInMemory, lruBlock) // this may or may not be present, depending on if accessedBlock call went through yet
		}
		err := os.Remove(this.blockPath(lruBlock))
		if err != nil {
			if os.IsNotExist(err) {
				Log.Warn.Printf("Block marked on disk but not found on disk %s", err.Error())
				lruBlock.OnDisk = false
				delete(this.cachedOnDisk, lruBlock)

			} else {
				Log.Error.Printf("Error while deleting block from disk: %s", err.Error())
			}
		} else {
			lruBlock.OnDisk = false
			delete(this.cachedOnDisk, lruBlock)
		}
		lruBlock.mu.Unlock()
	}
}

func (this *ObjCache) PrepareAnnounce(callback prepareAnnounceCallback) {
	this.mu.Lock()
	announceFiles := make([]AnnounceFile, 0)

	for _, file := range this.files {
		indexes := make([]int, 0)
		for _, block := range file.Blocks {
			if block.OnDisk {
				indexes = append(indexes, block.Index)
			}
		}

		if len(indexes) > 0 {
			announceFile := AnnounceFile{
				Hash: file.PathHash,
				Length: file.Length,
				NumBlocks: file.NumBlocks,
				BlockSize: file.BlockSize,
				Indexes: indexes,
			}
			announceFiles = append(announceFiles, announceFile)
		}
	}
	this.mu.Unlock()

	callback(announceFiles)
}

func (this *ObjCache) Load() {
	// exit if cacheLocation doesn't exist
	if _, err := os.Stat(this.cacheLocation); os.IsNotExist(err) {
		Log.Error.Printf("Could not load: %s does not exist", this.cacheLocation)
		os.Exit(1)
	}

	// create subdirectories if they don't exist already
	for _, subtype := range []string{"meta", "obj"} {
		testDir := filepath.Join(this.cacheLocation, subtype, "00")
		if _, err := os.Stat(testDir); os.IsNotExist(err) {
			Log.Info.Printf("%s directories missing, creating them now", subtype)
			for i := 0; i < 256; i++ {
				createDir := filepath.Join(this.cacheLocation, subtype, string(HEXADECIMAL_CHARS[i / 16]) + string(HEXADECIMAL_CHARS[i % 16]))
				err := os.MkdirAll(createDir, 0755)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	// scan the cacheLocation for .meta and .obj files, and add them to our structures
	// first look for .meta
	filepath.Walk(filepath.Join(this.cacheLocation, "meta"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			Log.Warn.Printf("Could not scan into %s", path)
			return nil
		}

		if info.Mode().IsRegular() && strings.HasSuffix(path, ".meta") {
			// verify correct subdirectory / filename format
			dirName := filepath.Base(filepath.Dir(path))
			fileName := filepath.Base(path)
			if len(dirName) != 2 || !strings.HasPrefix(fileName, dirName) {
				Log.Warn.Printf("Skipping invalid metadata filename %s", path)
				return nil
			}

			// read the metadata and append to file list
			metaString, err := ioutil.ReadFile(path)
			if err != nil {
				Log.Warn.Printf("Error while reading [%s]: %s", path, err.Error())
				return nil
			}
			parts := strings.Split(string(metaString), ":")

			if len(parts) != 4 {
				Log.Warn.Printf("Error while processing [%s]: metadata does not contain exactly four parts", path)
				return nil
			} else if parts[0] != strings.Split(fileName, ".meta")[0] {
				Log.Warn.Printf("Error while processing [%s]: hash in metadata mismatch with filename", path)
				return nil
			}

			metaLength, err1 := strconv.ParseInt(parts[1], 10, 64)
			numBlocks, err2 := strconv.ParseUint(parts[2], 10, 16)
			blockSize, err2 := strconv.ParseUint(parts[3], 10, 32)
			if err1 != nil || err2 != nil {
				Log.Warn.Printf("Error while processing [%s]: metadata contains invalid file length or number blocks", path)
				return nil
			}

			this.appendFile(parts[0], metaLength, uint16(numBlocks), uint32(blockSize))
		}

		return nil
	})

	// then look for .obj
	countBlocks := 0
	filepath.Walk(filepath.Join(this.cacheLocation, "obj"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			Log.Warn.Printf("Could not scan into %s", path)
			return nil
		}

		if info.Mode().IsRegular() && strings.HasSuffix(path, ".obj") {
			// verify correct subdirectory / filename format
			dirName := filepath.Base(filepath.Dir(path))
			fileName := filepath.Base(path)
			if len(dirName) != 2 {
				Log.Warn.Printf("Skipping invalid obj filename %s", path)
				return nil
			}

			// parse hash_idx.obj => hash, idx
			nameParts := strings.Split(strings.Split(fileName, ".obj")[0], "_")
			if len(nameParts) != 2 {
				Log.Warn.Printf("Error while processing [%s]: filename has bad format (expected hash_idx.obj)", path)
				return nil
			}

			blockFile := nameParts[0]
			blockIndex, err := strconv.ParseInt(nameParts[1], 10, 32)
			if err != nil {
				Log.Warn.Printf("Error while processing [%s]: bad block index in filename", path)
				return nil
			}

			// make sure subdirectory matches what we expect
			//  (block files go in subdirectory according to hash of file pathhash / block index)
			expectedSubdir := hexHash(fmt.Sprintf("%s%d", blockFile, blockIndex))[0:2]
			if expectedSubdir != dirName {
				Log.Warn.Printf("Error while processing [%s]: block in unexpected subdirectory", path)
				return nil
			}

			// make sure file exists, and associate with that file
			cacheFile, ok := this.files[blockFile]
			if !ok {
				Log.Warn.Printf("Error while processing [%s]: no .meta for file [%s]", path, blockFile)
				return nil
			}
			if blockIndex < 0 || blockIndex >= int64(len(cacheFile.Blocks)) {
				Log.Warn.Printf("Error while processing [%s]: index out of bounds (corresponding file has %d blocks)", path, len(cacheFile.Blocks))
				return nil
			}

			cacheFile.Blocks[blockIndex].OnDisk = true
			countBlocks++
		}

		return nil
	})

	Log.Info.Printf("Loaded %d files and %d blocks", len(this.files), countBlocks)
}

func (this *ObjCache) RegisterFile(filePath string, path string) bool {
	fin, err := os.Open(filePath)
	if err != nil {
		Log.Error.Printf("Error encountered while reading from file [%s]: %s", filePath, err.Error())
		return false
	}
	defer fin.Close()

	buf := make([]byte, BLOCK_SIZE)
	pathHash := hexHash(path)
	length := 0
	index := 0

	for {
		readCount, err := fin.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			Log.Error.Printf("Error encountered while reading from file [%s]: %s", filePath, err.Error())
			return false
		}

		// commit bytes to next object file
		objSubdir := hexHash(fmt.Sprintf("%s%d", pathHash, index))
		objPath := filepath.Join(this.cacheLocation, "obj", objSubdir[0:2], fmt.Sprintf("%s_%d.obj", pathHash, index))
		err = ioutil.WriteFile(objPath, buf[:readCount], 0644)
		if err != nil {
			Log.Error.Printf("Error encountered while writing to [%s] for file registration: %s", objPath, err.Error())
			return false
		}

		length += readCount
		index++
		if readCount < BLOCK_SIZE {
			break
		}
	}

	// create .meta file
	metaString := fmt.Sprintf("%s:%d:%d:%d", pathHash, length, index, BLOCK_SIZE)
	metaFile := filepath.Join(this.cacheLocation, "meta", pathHash[0:2], fmt.Sprintf("%s.meta", pathHash))
	err = ioutil.WriteFile(metaFile, []byte(metaString), 0644)
	if err != nil {
		Log.Error.Printf("Failed to write file metadata to [%s]: %s", metaFile, err.Error())
		return false
	}

	this.mu.Lock()
	cacheFile := this.appendFile(pathHash, int64(length), uint16(index), BLOCK_SIZE)
	for _, block := range cacheFile.Blocks {
		// no need to lock on block -- we still have this.mu locked, so no one could have pointer to the file yet
		block.OnDisk = true
	}
	this.mu.Unlock()
	return true
}
