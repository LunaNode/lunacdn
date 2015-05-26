package lunacdn

import "net/http"
import "fmt"
import "strings"
import "sync"
import "time"
import "os"
import "errors"
import "io"

type Serve struct {
	mu sync.Mutex
	cache Cache

	// map from path to error time of recent files that failed to fully download
	recentErrors map[string]time.Time
}

type CacheReader struct {
	cache Cache
	serve *Serve
	path string
	file CacheFile
	offset int64

	currentBlock []byte
	currentBlockOffset int64
}

func MakeServe(cfg *Config, cache Cache) *Serve {
	this := new(Serve)
	this.cache = cache
	this.recentErrors = make(map[string]time.Time)

	http.HandleFunc("/", this.handler)
	Log.Info.Printf("Starting HTTP server on [%s]", cfg.HttpBind)
	err := http.ListenAndServe(cfg.HttpBind, nil)
	if err != nil {
		Log.Error.Printf("Failed to initialize HTTP socket: %s", err.Error())
		panic(err)
	}

	return this
}

func (this *Serve) handler(w http.ResponseWriter, r *http.Request) {
	// convert from path like "/file" to "file"
	path := r.URL.Path
	shortPath := path[1 : len(path)]
	pathParts := strings.Split(shortPath, "/")

	// check if we recently error'd on this file, in which case show 500
	noError := this.checkError(shortPath)
	if !noError {
		this.temporarilyUnavailable(w)
		Log.Debug.Printf("Request for [%s]: found in error cache", shortPath)
		return
	}

	// grab the CacheFile object for reading
	file, err := this.cache.DownloadInit(shortPath)
	if err != nil {
		this.notFound(w)
		Log.Debug.Printf("Request for [%s]: file not found", shortPath)
		return
	}

	// pass to ServeContent to handle the complicated stuff
	Log.Debug.Printf("Request for [%s]: in progress", shortPath)
	reader := MakeCacheReader(this.cache, this, shortPath, file)
	http.ServeContent(w, r, pathParts[len(pathParts) - 1], time.Time{}, reader)
}

func (this *Serve) reportError(path string) {
	this.mu.Lock()
	this.recentErrors[path] = time.Now()
	this.mu.Unlock()
}

func (this *Serve) checkError(path string) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	errorTime, ok := this.recentErrors[path]
	if !ok {
		return true
	} else {
		if time.Now().After(errorTime.Add(DOWNLOAD_ERROR_CACHE_TIME)) {
			delete(this.recentErrors, path)
			return true
		} else {
			return false
		}
	}
}

func (this *Serve) notFound(w http.ResponseWriter) {
	w.WriteHeader(404)
	fmt.Fprint(w, "404")
}

func (this *Serve) temporarilyUnavailable(w http.ResponseWriter) {
	w.WriteHeader(503)
	fmt.Fprint(w, "503")
}

func MakeCacheReader(cache Cache, serve *Serve, path string, file CacheFile) *CacheReader {
	this := new(CacheReader)
	this.cache = cache
	this.serve = serve
	this.path = path
	this.file = file
	this.offset = 0
	return this
}

func (this *CacheReader) Seek(offset int64, whence int) (int64, error) {
	actualOffset := offset
	if whence == os.SEEK_CUR {
		actualOffset = this.offset + offset
	} else if whence == os.SEEK_END {
		actualOffset = this.file.GetLength() + offset
	}

	if actualOffset < 0 {
		return this.offset, errors.New("seek to negative offset")
	}

	this.offset = actualOffset
	return this.offset, nil
}

func (this *CacheReader) Read(p []byte) (int, error) {
	if this.offset >= this.file.GetLength() {
		return 0, io.EOF
	}

	pOffset := 0
	t := 0

	for {
		t++
		if t > 5 {
			os.Exit(1)
		}
		if this.currentBlock != nil && this.offset >= this.currentBlockOffset && this.offset < this.currentBlockOffset + int64(len(this.currentBlock)) {
			copyBytes := copy(this.currentBlock[this.offset - this.currentBlockOffset:], p[pOffset:])
			this.offset += int64(copyBytes)
			pOffset += copyBytes
		}

		if pOffset < len(p) && this.offset < this.file.GetLength() {
			// need to read more bytes, load the next block
			blockIndex := int(this.offset / int64(this.file.GetBlockSize()))
			this.currentBlock = this.cache.DownloadRead(this.file, blockIndex, true)
			this.currentBlockOffset = int64(blockIndex) * int64(this.file.GetBlockSize())

			if this.currentBlock == nil {
				// add to error cache since this download failed apparently
				this.serve.reportError(this.path)
				return 0, errors.New(fmt.Sprintf("failed to read block %d", blockIndex))
			}
		} else {
			break
		}
	}

	return pOffset, nil
}
