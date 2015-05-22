package lunacdn

import "net/http"
import "fmt"
import "strings"

type Serve struct {
	cache *Cache

}

func MakeServe(cfg *Config, cache *Cache) *Serve {
	this := new(Serve)
	this.cache = cache

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

	// grab the CacheFile object for reading
	file := this.cache.DownloadInit(shortPath)
	if file == nil {
		this.NotFound(w)
		Log.Debug.Printf("Request for [%s]: file not found", shortPath)
		return
	}

	// the file seems to exist, so let's try to read it block by block
	// we write each block to the HTTP connection as we retrieve it
	Log.Debug.Printf("Request for [%s]: in progress", shortPath)
	blockIndex := 0

	for {
		block := this.cache.DownloadRead(file, blockIndex, true)

		// if this is the first empty block, we should 404, otherwise just quit
		// if we see first non-empty block then set headers
		if block == nil {
			if blockIndex == 0 {
				this.NotFound(w)
			}
			break
		} else if blockIndex == 0 {
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", pathParts[len(pathParts) - 1]))
			w.Header().Set("Content-Type", "application/octet-stream")
		}

		w.Write(block)
		blockIndex++
	}
}

func (this *Serve) NotFound(w http.ResponseWriter) {
	w.WriteHeader(404)
	fmt.Fprint(w, "404")
}
