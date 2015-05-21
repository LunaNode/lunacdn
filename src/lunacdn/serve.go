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
	path := r.URL.Path
	shortPath := path[1 : len(path)]
	pathParts := strings.Split(shortPath, "/")

	file := this.cache.DownloadInit(shortPath)
	if file == nil {
		this.NotFound(w)
		Log.Debug.Printf("Request for [%s]: file not found", shortPath)
		return
	}

	Log.Debug.Printf("Request for [%s]: in progress", shortPath)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", pathParts[len(pathParts) - 1]))
	w.Header().Set("Content-Type", "application/octet-stream")

	blockIndex := 0
	for {
		block := this.cache.DownloadRead(file, blockIndex, true)
		w.Write(block)

		if block == nil {
			break
		}

		blockIndex++
	}

	if blockIndex == 0 {
		this.NotFound(w)
	}
}

func (this *Serve) NotFound(w http.ResponseWriter) {
	w.WriteHeader(404)
	fmt.Fprint(w, "404")
}
