package lunacdn

import "log"
import "io"
import "os"

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

func Run() {
	InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
	cfg := LoadConfig()
	peerList := MakePeerList(cfg)
	cache := MakeCache(cfg, peerList)
	peerList.SetCache(cache)
	MakeServe(cfg, cache)
}
