package lunacdn

import "net/http"
import "fmt"
import "strings"
import "sync"
import "sync/atomic"
import "time"

type Serve struct {
	mu sync.Mutex
	cache *Cache

	// map from path to error time of recent files that failed to fully download
	recentErrors map[string]time.Time
}

func MakeServe(cfg *Config, cache *Cache) *Serve {
	this := new(Serve)
	this.cache = cache
	this.recentErrors = make(map[string]time.Time)

	http.HandleFunc("/", this.Handler)
	Log.Info.Printf("Starting HTTP server on [%s]", cfg.HttpBind)
	err := http.ListenAndServe(cfg.HttpBind, nil)
	if err != nil {
		Log.Error.Printf("Failed to initialize HTTP socket: %s", err.Error())
		panic(err)
	}

	return this
}

func (this *Serve) Handler(w http.ResponseWriter, r *http.Request) {
	// convert from path like "/file" to "file"
	path := r.URL.Path
	shortPath := path[1 : len(path)]
	pathParts := strings.Split(shortPath, "/")

	// check if we recently error'd on this file, in which case show 500
	noError := this.CheckError(shortPath)
	if !noError {
		this.TemporarilyUnavailable(w)
		Log.Debug.Printf("Request for [%s]: found in error cache", shortPath)
		return
	}

	// grab the CacheFile object for reading
	file := this.cache.DownloadInit(shortPath)
	if file == nil {
		this.NotFound(w)
		Log.Debug.Printf("Request for [%s]: file not found", shortPath)
		return
	}

	// the file seems to exist, so let's try to read it block by block
	// we read the blocks from our cache in a separate goroutine from where we write to the client
	//  this is necessary since we don't want cache misses to stall client-server communication
	// to synchronize, we:
	//  a) use a buffered channel to send blocks from reader to writer
	//      the size of the buffer is the maximum number of blocks to keep temporarily
	//  b) the writer may terminate if there's an error; reader sees this by timing out
	//      on reads and checking atomic boolean
	Log.Debug.Printf("Request for [%s]: in progress", shortPath)
	blockChannel := make(chan []byte, SERVE_BUFFER_BLOCKS)
	terminated := new(int32)
	*terminated = 0

	go func() {
		blockIndex := 0
		quit := false

		for !quit {
			block := this.cache.DownloadRead(file, blockIndex, true)
			blockIndex++
			written := false

			for !written && !quit {
				select {
				case blockChannel <- block:
					written = true
				case <- time.After(time.Second):
					if atomic.LoadInt32(terminated) != 0 {
						quit = true
					}
				}
			}

			if block == nil {
				break
			}
		}
	}()

	firstBlock := true
	var writtenLength int64 = 0
	for {
		block := <- blockChannel

		// if this is the first empty block, we should 404, otherwise it probably means we're done so just quit
		// if we see first non-empty block then set headers
		if block == nil {
			if firstBlock {
				this.TemporarilyUnavailable(w)
			}

			// add to error cache if this download failed (didn't get all the bytes)
			if writtenLength != file.Length {
				Log.Warn.Printf("Failed to serve %s (len=%d but only wrote %d)", shortPath, file.Length, writtenLength)
				this.mu.Lock()
				this.recentErrors[shortPath] = time.Now()
				this.mu.Unlock()
			}
			break
		} else if firstBlock {
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", pathParts[len(pathParts) - 1]))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", file.Length))
			firstBlock = false
		}

		_, err := w.Write(block)
		writtenLength += int64(len(block))

		if err != nil {
			Log.Debug.Printf("Failed to write block to HTTP: %s", err.Error())
			atomic.StoreInt32(terminated, 1)
			break
		}
	}
}

func (this *Serve) CheckError(path string) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	errorTime, ok := this.recentErrors[path]
	if !ok {
		return true
	} else {
		if time.Now().After(errorTime.Add(DOWNLOAD_ERROR_CACHE_TIME * time.Second)) {
			delete(this.recentErrors, path)
			return true
		} else {
			return false
		}
	}
}

func (this *Serve) NotFound(w http.ResponseWriter) {
	w.WriteHeader(404)
	fmt.Fprint(w, "404")
}

func (this *Serve) TemporarilyUnavailable(w http.ResponseWriter) {
	w.WriteHeader(503)
	fmt.Fprint(w, "503")
}
