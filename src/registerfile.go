package main

import "os"
import "fmt"
import "lunacdn"

func main() {
	if len(os.Args) != 4 {
		fmt.Println("registerfile cfg file url_path (e.g. registerfile lunacdn.cfg /home/user/blah.dat files/blah.dat)")
		os.Exit(-1)
	}

	lunacdn.InitLogging(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
	cfg := lunacdn.LoadConfig(os.Args[1])
	cache := lunacdn.MakeCache(cfg, nil)
	cache.RegisterFile(os.Args[2], os.Args[3])
}
