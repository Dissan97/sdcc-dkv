package main

import (
	"client/cli_interface"
	"client/test_dkv"
	"flag"
	"log"
	"strings"
)

func main() {
	servers := flag.String("servers", "localhost:8080;localhost:8081;localhost:8082", "server addresses")
	mode := flag.String("mode", "normal", "client mode")
	filename := flag.String("t_filename", "resources/docker-compose.yaml", "test file docker-compose.yaml")
	threads := flag.Int("threads", 100, "number of threads for stress test")
	flag.Parse()

	if *servers == "" {
		log.Fatalln("No servers specified")
	}

	if *mode != "normal" && *mode != "test" {
		log.Fatalln("Invalid mode:", *mode)
	}

	if *mode == "normal" {
		cli := new(cli_interface.CliInterface)
		cli.Init(*servers)
		cli.Run()
		return
	}
	testEnv := new(test_dkv.TestEnv)
	testEnv.Init(strings.Split(*servers, ";"), *filename, *threads)
	testEnv.TestFunctions()
	testEnv.StressTestPut()

}
