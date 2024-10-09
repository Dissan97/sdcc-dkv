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
	test_env := new(test_dkv.TestEnv)
	test_env.Init(strings.Split(*servers, ";"), *filename)
	test_env.TestFunctions()
	test_env.StressTest()

}
