// File Client: client/main.go
package main

import (
	// "crypto/rand"

	"fmt"
	"log"
	"net/rpc"
	"strconv"

	"bufio"
	"os"
	"strings"

	. "lab01_rpc/common"
)

func handleCommand(client *rpc.Client, command string) {
	parts := strings.Fields(command)
	if len(parts) < 1 {
		fmt.Println("Invalid command. Please try again.")
		return
	}

	switch parts[0] {
	case "SET":
		if len(parts) < 4 {
			fmt.Println("Usage: SET <bucket> <key> <value>")
			return
		}

		bucket := parts[1]
		key, _ := strconv.Atoi(parts[2])
		value := parts[3]

		args := &SetArgs{
			Bucket: bucket,
			Key:    key,
			Value:  value,
		}

		var reply string
		err := client.Call("FastDB.Set", args, &reply)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println("Reply from server:", reply)

	case "GET":
		if len(parts) < 3 {
			fmt.Println("Usage: get <bucket> <key>")
			return
		}

		bucket := parts[1]
		key, _ := strconv.Atoi(parts[2])

		args := &GetArgs{
			Bucket: bucket,
			Key:    key,
		}
		var reply string
		err := client.Call("FastDB.Get", args, &reply)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println("Reply from server:", reply)

	case "GETALL":
		if len(parts) < 2 {
			fmt.Println("Usage: GETALL <bucket>")
			return
		}

		bucket := parts[1]

		args := &GetAllArgs{
			Bucket: bucket,
		}
		var reply map[int]string
		err := client.Call("FastDB.GetAll", args, &reply)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		for key, value := range reply {
			fmt.Println("Reply from server: Key: " + strconv.Itoa(key) + ", Value: " + value)
		}

	case "DELETE":
		if len(parts) < 3 {
			fmt.Println("Usage: DELETE <bucket> <key>")
			return
		}

		bucket := parts[1]
		key, _ := strconv.Atoi(parts[2])

		args := &DeleteArgs{
			Bucket: bucket,
			Key:    key,
		}
		var reply string
		err := client.Call("FastDB.Delete", args, &reply)

		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println("Reply from server:", reply)

	default:
		fmt.Println("Unknown command. Available commands: set, get")
	}
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to RPC server. Type 'SET' or 'GET' to interact.")
	fmt.Println("Usage examples:")
	fmt.Println("  SET <bucket> <key> <value>")
	fmt.Println("  GET <bucket> <key>")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		command := scanner.Text()
		handleCommand(client, command)
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error reading input:", err)
	}

}
