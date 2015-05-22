package main

import "lunacdn"
import "os"

func main() {
	cfgPath := "lunacdn.cfg"
	if len(os.Args) >= 2 {
		cfgPath = os.Args[1]
	}

	lunacdn.Run(cfgPath)
}
