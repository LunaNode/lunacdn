package lunacdn

/*
cache_file.go: implements Cache interface for store directory where we just put the files (may be in subdirectories).
We scan the filesystem periodically to maintain a map from hashes to file paths (maybe we can improve on this redundancy in the future?).
*/

import "sync"
import "path/filepath"
import "strings"
import "errors"
import "time"
import "os"
import "io"

type FCFile struct {
	Path string
	Length int64
}

// implement CacheFile interface
func (this FCFile) GetPathHash() string {
	return hexHash(this.Path)
}
func (this FCFile) GetLength() int64 {
	return this.Length
}
func (this FCFile) GetNumBlocks() uint16 {
	return uint16((this.Length + BLOCK_SIZE - 1) / BLOCK_SIZE)
}
func (this FCFile) GetBlockSize() uint32 {
	return BLOCK_SIZE
}

type FileCache struct {
	mu sync.Mutex
	cacheLocation string
	files map[string]FCFile // keys are path hashes
}

func MakeFileCache(cfg *Config) *FileCache {
	this := new(FileCache)
	this.cacheLocation = cfg.CachePath
	this.files = make(map[string]FCFile)

	go func() {
		for {
			this.Load()
			time.Sleep(FILECACHE_SCAN_INTERVAL)
		}
	}()

	return this
}

func (this *FileCache) NotifyFile(hash string, length int64, umBlocks uint16, blockSize uint32) {
	// ignore
}

func (this *FileCache) DownloadInit(path string) (CacheFile, error) {
	return this.DownloadInitHash(hexHash(path))
}

func (this *FileCache) DownloadInitHash(pathHash string) (CacheFile, error) {
	this.mu.Lock()
	file, ok := this.files[pathHash]
	this.mu.Unlock()

	if !ok {
		return file, errors.New("file not found")
	} else {
		return file, nil
	}
}

func (this *FileCache) DownloadRead(cacheFile CacheFile, index int, tryPeers bool) []byte {
	file, ok := cacheFile.(FCFile)
	if !ok {
		Log.Error.Printf("DownloadRead got bad request, cacheFile is not FCFile type")
		return nil
	}
	if index >= int(file.GetNumBlocks()) {
		return nil
	}

	fin, err := os.Open(this.cacheLocation + "/" + file.Path)
	if err != nil {
		Log.Warn.Printf("Error reading %s: %s", file.Path, err.Error())
		return nil
	}
	defer fin.Close()

	b := make([]byte, BLOCK_SIZE)
	count, err := fin.ReadAt(b, int64(index) * BLOCK_SIZE)
	if err != nil && err != io.EOF {
		Log.Warn.Printf("Error reading %s: %s", file.Path, err.Error())
		return nil
	}

	return b[:count]
}

func (this *FileCache) PrepareAnnounce(callback prepareAnnounceCallback) {
	this.mu.Lock()
	announceFiles := make([]AnnounceFile, 0, len(this.files))

	for _, file := range this.files {
		indexes := make([]int, file.GetNumBlocks())
		for i := range indexes {
			indexes[i] = i
		}

		announceFile := AnnounceFile{
			Hash: file.GetPathHash(),
			Length: file.GetLength(),
			NumBlocks: file.GetNumBlocks(),
			BlockSize: file.GetBlockSize(),
			Indexes: indexes,
		}
		announceFiles = append(announceFiles, announceFile)
	}
	this.mu.Unlock()

	callback(announceFiles)
}

func (this *FileCache) Load() {
	files := make(map[string]FCFile)
	filepath.Walk(this.cacheLocation, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			Log.Warn.Printf("Could not scan into %s", path)
			return nil
		}

		if info.Mode().IsRegular() {
			pathParts := strings.Split(path, this.cacheLocation + "/")

			if len(pathParts) == 2 {
				subPath := pathParts[1]
				Log.Debug.Printf("Registering file %s", subPath)
				files[hexHash(subPath)] = FCFile{Path: subPath, Length: info.Size()}
			}
		}

		return nil
	})

	this.mu.Lock()
	this.files = files
	this.mu.Unlock()
}
