package config

import (
	"log"
	"net/rpc"
)

func ConnectRPC(address string) *rpc.Client {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Fatalf("Error connecting to RPC server: %v", err)
	}
	return client
}
