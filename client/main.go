package main

import (
	"bufio"
	"client/utils"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const lines = "_____________________"

type CliInterface struct {
	Servers     []string
	Commands    []string
	inputReader *bufio.Reader
}

func (cli *CliInterface) put() {
	server := ""
	key := ""
	value := ""
	var buf []byte
	var err error

	fmt.Println(cli.getPageName("PUT"))

	server, err = cli.getServerFromInput()
	if err != nil {
		if cli.tryAgain("put") {
			cli.put()
		}
		return
	}
	fmt.Print("insert key>> ")
	buf, _, err = cli.inputReader.ReadLine()
	if err != nil {
		log.Fatal("put input error: ", err)
		return
	}
	key = string(buf)
	fmt.Print("insert value>> ")
	buf, _, err = cli.inputReader.ReadLine()
	if err != nil {
		log.Fatal("put input error: ", err)
		return
	}
	value = string(buf)

	if server == "" || key == "" || value == "" {
		fmt.Printf("put input error, server:%s key:%s value:%s\n", server, key, value)
		if cli.tryAgain("put") {
			cli.put()
		}
		return
	}

	if err = utils.PerformPut(server, key, value); err != nil {
		fmt.Printf("put input error, server:%s key:%s value:%s\n", server, key, value)
	}

}

func (cli *CliInterface) get() {
	server := ""
	key := ""
	var buf []byte
	var err error

	fmt.Println(cli.getPageName("GET"))

	server, err = cli.getServerFromInput()
	if err != nil {
		if cli.tryAgain("get") {
			cli.get()
		}
		return
	}
	fmt.Print("insert key>> ")
	buf, _, err = cli.inputReader.ReadLine()
	if err != nil {
		log.Fatal("get input error: ", err)
		return
	}
	key = string(buf)

	if server == "" || key == "" {
		fmt.Printf("get input error, server:%s key:%s\n", server, key)
		if cli.tryAgain("get") {
			cli.get()
		}
		return
	}

	if err = utils.PerformGet(server, key); err != nil {
		fmt.Printf("get input error, server:%s key:%s\n", server, key)
	}
}

func (cli *CliInterface) del() {
	server := ""
	key := ""
	var buf []byte
	var err error

	fmt.Println(cli.getPageName("DELETE"))

	server, err = cli.getServerFromInput()
	if err != nil {
		if cli.tryAgain("del") {
			cli.del()
		}
		return
	}
	fmt.Print("insert key>> ")
	buf, _, err = cli.inputReader.ReadLine()
	if err != nil {
		log.Fatal("del input error: ", err)
	}
	key = string(buf)

	if server == "" || key == "" {
		fmt.Printf("del input error, server:%s key:%s\n", server, key)
		if cli.tryAgain("del") {
			cli.del()
		}
		return
	}

	if err = utils.PerformDelete(server, key); err != nil {
		fmt.Printf("del input error, server:%s key:%s\n", server, key)
	}
}

func (cli *CliInterface) getPageName(value string) string {
	return lines + value + lines
}

func (cli *CliInterface) showHelp() {
	output := cli.getPageName("HELP PAGE") + "\n" +
		"you can either choose the index or type command name\n" +
		"command are not case sensitive\n" +
		"available command list:\n"

	for index, command := range cli.Commands {
		output += fmt.Sprintf("\t[%d] %s\n", index, command)
	}
	fmt.Print(output)
}

func (cli *CliInterface) run() {

	fmt.Println("Welcome to the DKV client")
	cli.showHelp()
	for {

		fmt.Print(cli.getPageName("HOME PAGE") + "\n" +
			"insert a command>> ")
		buf, _, err := cli.inputReader.ReadLine()
		if err != nil {
			log.Fatal("Readline:", err)
		}

		switch strings.ToLower(string(buf)) {
		case "0", "put", "p":
			cli.put()
		case "1", "get", "g":
			cli.get()
		case "2", "del", "d":
			cli.del()
		case "3", "help", "h":
			cli.showHelp()
		case "4", "exit", "e", "quit", "q":
			fmt.Println("thank you for using the application bye...")
			os.Exit(0)
		default:
			fmt.Println("not recognized command", string(buf))
			cli.showHelp()
		}
	}
}

func (cli *CliInterface) Init(servers string) {
	cli.Servers = strings.Split(servers, ";")
	for _, server := range cli.Servers {
		if !strings.Contains(server, ":") {
			log.Fatal("this server format not allowed", server)
		}
		tmp := strings.Split(server, ":")
		_, err := strconv.Atoi(tmp[1])
		if err != nil {
			log.Fatal("this server port number not allowed", tmp[1])
		}
	}

	cli.Commands = make([]string, 5)
	cli.Commands[0] = "put | p"
	cli.Commands[1] = "get | g"
	cli.Commands[2] = "del | d"
	cli.Commands[3] = "help | h"
	cli.Commands[4] = "exit | e | quit | q"
	cli.inputReader = bufio.NewReader(os.Stdin)
}

func (cli *CliInterface) getServerFromInput() (string, error) {
	cli.availableServers()
	fmt.Print("Choose a server by index or name>> ")
	buf, _, err := cli.inputReader.ReadLine()
	if err != nil {
		log.Fatal("getServerFromInput error: ", err)
	}
	input := strings.ToLower(string(buf))

	// Check if input is a valid index
	index, err := strconv.Atoi(input)
	if err == nil && index >= 0 && index < len(cli.Servers) {
		return cli.Servers[index], nil
	}

	// Check if input is a valid server name
	for _, server := range cli.Servers {
		if strings.ToLower(server) == input {
			return server, nil
		}
	}

	// If no match, show bad server choice message
	fmt.Printf("Bad server choice: %s\n", input)
	cli.availableServers()
	return "", fmt.Errorf("invalid server selection")
}

func (cli *CliInterface) availableServers() {

	fmt.Println("Available servers:")
	for index, srv := range cli.Servers {
		fmt.Printf("[%d] %s\n", index, srv)
	}
}

func (cli *CliInterface) tryAgain(s string) bool {
	fmt.Printf("function %s error try again? y - yes to continue>> ", s)
	buf, _, err := cli.inputReader.ReadLine()
	if err != nil {
		log.Fatal("put input error: ", err)
	}
	dummy := strings.ToLower(string(buf))
	if dummy == "y" || dummy == "yes" {
		return true
	}
	return false
}

func main() {
	servers := flag.String("servers", "localhost:8080", "server addresses")
	flag.Parse()

	if *servers == "" {
		log.Println("No servers specified")
		return
	}
	cli := new(CliInterface)
	cli.Init(*servers)
	cli.run()
}
