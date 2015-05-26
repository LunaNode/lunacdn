package lunacdn

import "log"
import "io"
import "os"
import "math/rand"
import "time"

var Log struct {
	Debug *log.Logger
	Info *log.Logger
	Warn *log.Logger
	Error *log.Logger
}

var Debug bool

func InitLogging(debugHandle io.Writer, infoHandle io.Writer, warnHandle io.Writer, errorHandle io.Writer) {
	Log.Debug = log.New(debugHandle, "DEBUG: ", log.Ldate | log.Ltime | log.Lshortfile)
	Log.Info = log.New(infoHandle, "INFO: ", log.Ldate | log.Ltime | log.Lshortfile)
	Log.Warn = log.New(warnHandle, "WARN: ", log.Ldate | log.Ltime | log.Lshortfile)
	Log.Error = log.New(errorHandle, "ERROR: ", log.Ldate | log.Ltime | log.Lshortfile)
	Debug = true
}

func Run(cfgPath string) {
	rand.Seed(time.Now().UTC().UnixNano())
	InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr)

	cfg := LoadConfig(cfgPath)
	exitChannel := make(chan bool)
	peerList := MakePeerList(cfg, exitChannel)

	var cache Cache
	if cfg.CacheAsFile {
	} else {
		cache = MakeObjCache(cfg, peerList)
	}

	peerList.SetCache(cache)

	if cfg.ModeServe {
		MakeServe(cfg, cache)
	}

	<- exitChannel
}
