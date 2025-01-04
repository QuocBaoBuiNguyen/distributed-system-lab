// File Client: client/main.go
package main

import (
	"fmt"
	"log"

	"lab01_rpc/client/commands"
	"lab01_rpc/client/config"
	"lab01_rpc/client/dispatcher"
)

func main() {

	rpcClient := config.ConnectRPC("localhost:1234")
	defer rpcClient.Close()

	scanner := config.StartCLI()

	cmdHandlers := commands.NewCommand(rpcClient)

	dispatch := dispatcher.InitializeDispatcher()
	dispatch.RegisterCommand("SET", cmdHandlers.HandlerSetCommand())
	dispatch.RegisterCommand("GET", cmdHandlers.HandlerGetCommand())
	dispatch.RegisterCommand("GETALL", cmdHandlers.HandlerGetAllCommand())
	dispatch.RegisterCommand("DELETE", cmdHandlers.HandlerDeleteCommand())

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		command := scanner.Text()
		dispatch.Handle(command)
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error reading input:", err)
	}

}
