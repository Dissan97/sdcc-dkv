package utils

import (
	"log"
	"net/rpc"
)

func CloseClient(client *rpc.Client) {
	if client != nil {
		err := client.Close()
		if err != nil {
			log.Printf("%s ", err.Error())
			return
		}
	}
}
