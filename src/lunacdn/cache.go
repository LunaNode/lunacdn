package lunacdn

/*
cache.go: caches blocks on disk and in memory, and serves to peers and downloaders
On cache miss, we try to serve the block from a peer via PeerList instance.
*/

import "sync"
import "time"
import "crypto/md5"
import "encoding/hex"
import "fmt"
import "io/ioutil"
import "os"
import "strconv"
import "strings"

/*
Synchronization strategy:
  - CacheFile are not modified once registered
  - Lock on the block to modify OnDisk/Data
  - Always acquire global lock first, then block lock
*/

type CacheBlock struct {
	mu sync.Mutex
	File *CacheFile
	Index int
	Offset int64
	OnDisk bool

	// cached contents, or nil
	Data []byte
}

/*
 * Files that are registered with the cache.
 * We keep these objects permanently in memory, so the files may not actually be available.
 * CacheFile must not be modified once added to the Cache.files map
 */
type CacheFile struct {
	PathHash string
	Length int64
	Blocks []*CacheBlock
}

func pathToHash(path string) string {
	hashArray := md5.Sum([]byte(path))
	return hex.EncodeToString(hashArray[:])
}

type Cache struct {
	mu sync.Mutex

	// stored files, map from hashed file path to file object
	files map[string]*CacheFile

	// blocks currently stored in-memory or on-disk
	cachedInMemory map[*CacheBlock]time.Time
	cachedOnDisk map[*CacheBlock]time.Time

	memoryLimit int
	diskLimit int
	cacheLocation string
	peerList *PeerList
}

func MakeCache(cfg *Config, peerList *PeerList) *Cache {
	this := new(Cache)
	this.files = make(map[string]*CacheFile)
	this.peerList = peerList
	this.cachedInMemory = make(map[*CacheBlock]time.Time)
	this.cachedOnDisk = make(map[*CacheBlock]time.Time)

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

func (this *Cache) blockPath(block *CacheBlock) string {
	return fmt.Sprintf("%s/%s_%d.obj", this.cacheLocation, block.File.PathHash, block.Index)
}

func (this *Cache) NotifyFile(hash string, length int64) {
	this.mu.Lock()
	defer this.mu.Unlock()

	// register new file only if not already registered
	_, ok := this.files[hash]
	if ok {
		return
	}

	file := this.appendFile(hash, length)

	// create a .meta file so we can retrieve the CacheFile data on restart
	metaString := fmt.Sprintf("%s:%d", file.PathHash, file.Length)
	metaFile := fmt.Sprintf("%s/%s.meta", this.cacheLocation, file.PathHash)
	err := ioutil.WriteFile(metaFile, []byte(metaString), 0644)
	if err != nil {
		Log.Warn.Printf("Failed to write file metadata to [%s]: %s", metaFile, err.Error())
	}
}

func (this *Cache) appendFile(hash string, length int64) *CacheFile {
	file := &CacheFile{PathHash: hash, Length: length}
	numBlocks := int((length + BLOCK_SIZE - 1) / BLOCK_SIZE) // number blocks needed to store the length
	for i := 0; i < numBlocks; i++ {
		block := CacheBlock{File: file, Index: i, Offset: int64(i) * BLOCK_SIZE, OnDisk: false}
		file.Blocks = append(file.Blocks, &block)
	}
	this.files[hash] = file
	return file
}

func (this *Cache) DownloadInit(path string) *CacheFile {
	return this.DownloadInitHash(pathToHash(path))
}

func (this *Cache) DownloadInitHash(pathHash string) *CacheFile {
	this.mu.Lock()
	defer this.mu.Unlock()

	// check if we have the file registered in cache
	file := this.files[pathHash]
	return file // may be nil if we don't have it
}

func (this *Cache) DownloadRead(file *CacheFile, index int, tryPeers bool) []byte {
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
		bytes := this.peerList.DownloadBlock(file.PathHash, index, 30 * time.Second)

		if bytes != nil {
			block.Data = bytes
			err := ioutil.WriteFile(this.blockPath(block), bytes, 0644)
			if err != nil {
				Log.Warn.Printf("Failed to save block to filesystem: %s", err.Error())
			} else {
				block.OnDisk = true
			}
			return bytes
		}
	}

	return nil
}

// expects caller to have global lock
// we will not lock any other mutexes within this function
func (this *Cache) accessedBlock(updateBlock *CacheBlock) {
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

	for len(this.cachedOnDisk) > this.memoryLimit {
		var lruBlock *CacheBlock

		for block, time := range this.cachedInMemory {
			if lruBlock == nil || this.cachedInMemory[lruBlock].After(time) {
				lruBlock = block
			}
		}

		lruBlock.mu.Lock()
		lruBlock.Data = nil
		delete(this.cachedInMemory, lruBlock)
		lruBlock.mu.Unlock()
	}

	for this.diskLimit != -1 && len(this.cachedOnDisk) > this.diskLimit {
		var lruBlock *CacheBlock

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

type prepareAnnounceCallback func([]AnnounceFile)
func (this *Cache) PrepareAnnounce(callback prepareAnnounceCallback) {
	this.mu.Lock()
	defer this.mu.Unlock()

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
				Indexes: indexes,
			}
			announceFiles = append(announceFiles, announceFile)
		}
	}

	callback(announceFiles)
}

func (this *Cache) Load() {
	// scan the cacheLocation for .meta and .obj files, and add them to our structures
	files, err := ioutil.ReadDir(this.cacheLocation)

	if err != nil {
		Log.Error.Printf("Failed to list contents of cache location: %s", err.Error())
		return
	}

	// first pass: look for .meta
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".meta") {
			metaString, err := ioutil.ReadFile(this.cacheLocation + "/" + f.Name())
			if err != nil {
				Log.Warn.Printf("Error while reading [%s]: %s", f.Name(), err.Error())
				continue
			}
			parts := strings.Split(string(metaString), ":")

			if len(parts) != 2 {
				Log.Warn.Printf("Error while processing [%s]: metadata contains more than one colon", f.Name())
				continue
			}

			metaLength, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				Log.Warn.Printf("Error while processing [%s]: metadata contains invalid file length", f.Name())
				continue
			}

			this.appendFile(parts[0], metaLength)
		}
	}

	// second pass: look for .obj
	countBlocks := 0
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".obj") {
			// parse hash_idx.obj => hash, idx
			nameParts := strings.Split(strings.Split(f.Name(), ".obj")[0], "_")
			if len(nameParts) != 2 {
				Log.Warn.Printf("Error while processing [%s]: filename has bad format (expected hash_idx.obj): %s %d", f.Name())
				continue
			}

			blockFile := nameParts[0]
			blockIndex, err := strconv.ParseInt(nameParts[1], 10, 32)
			if err != nil {
				Log.Warn.Printf("Error while processing [%s]: bad block index in filename", f.Name())
				continue
			}

			cacheFile, ok := this.files[blockFile]
			if !ok {
				Log.Warn.Printf("Error while processing [%s]: no .meta for file [%s]", f.Name(), blockFile)
				continue
			}
			if blockIndex < 0 || blockIndex >= int64(len(cacheFile.Blocks)) {
				Log.Warn.Printf("Error while processing [%s]: index out of bounds (corresponding file has %d blocks)", f.Name(), len(cacheFile.Blocks))
				continue
			}

			cacheFile.Blocks[blockIndex].OnDisk = true
			countBlocks++
		}
	}

	Log.Info.Printf("Loaded %d files and %d blocks", len(this.files), countBlocks)
}

func (this *Cache) RegisterFile(filePath string, path string) bool {
	fin, err := os.Open(filePath)
	if err != nil {
		Log.Error.Printf("Error encountered while reading from file [%s]: %s", filePath, err.Error())
		return false
	}
	defer fin.Close()

	buf := make([]byte, BLOCK_SIZE)
	pathHash := pathToHash(path)
	length := 0
	index := 0

	for {
		readCount, err := fin.Read(buf)
		if err != nil {
			Log.Error.Printf("Error encountered while reading from file [%s]: %s", filePath, err.Error())
			return false
		}

		// commit bytes to next object file
		objPath := fmt.Sprintf("%s/%s_%d.obj", this.cacheLocation, pathHash, index)
		fout, err := os.Create(objPath)
		if err != nil {
			Log.Error.Printf("Error encountered while writing to [%s] for file registration: %s", objPath, err.Error())
			return false
		}

		_, err = fout.Write(buf[:readCount])
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
	metaString := fmt.Sprintf("%s:%d", pathHash, length)
	metaFile := fmt.Sprintf("%s/%s.meta", this.cacheLocation, pathHash)
	err = ioutil.WriteFile(metaFile, []byte(metaString), 0644)
	if err != nil {
		Log.Error.Printf("Failed to write file metadata to [%s]: %s", metaFile, err.Error())
		return false
	}

	this.mu.Lock()
	cacheFile := this.appendFile(pathHash, int64(length))
	for _, block := range cacheFile.Blocks {
		// no need to lock on block -- we still have this.mu locked, so no one could have pointer to the file yet
		block.OnDisk = true
	}
	this.mu.Unlock()
	return true
}
