package lunacdn

import "code.google.com/p/gcfg"

type Config struct {
	ModeNoDelete bool
	ModeServe bool

	HttpBind string
	BackendBind string
	BackendList string
	CachePath string
	CacheMemory int
	CacheDisk int
}

type ConfigMeta struct {
	Default Config
}

func LoadConfig() *Config {
	var cfg ConfigMeta
	err := gcfg.ReadFileInto(&cfg, "lunacdn.cfg")
	if err != nil {
		Log.Error.Printf("Error while reading configuration: %s", err.Error())
		panic(err)
	}
	return &cfg.Default
}
