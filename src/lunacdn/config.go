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
	CacheAsFile bool

	RedirectEnable bool
	RedirectGeoipPath string
	RedirectIP string
	RedirectLocation string
}

type ConfigMeta struct {
	Default Config
}

func LoadConfig(cfgPath string) *Config {
	var cfg ConfigMeta
	err := gcfg.ReadFileInto(&cfg, cfgPath)
	if err != nil {
		Log.Error.Printf("Error while reading configuration: %s", err.Error())
		panic(err)
	}
	return &cfg.Default
}
