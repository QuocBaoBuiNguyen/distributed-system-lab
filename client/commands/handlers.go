package commands

import (
	"fmt"
	"net/rpc"
	"strconv"

	"lab02_replication/common"
)

type CommandHandlers struct {
	Client *rpc.Client
}

func NewCommand(client *rpc.Client) *CommandHandlers {
	return &CommandHandlers{
		Client: client,
	}
}

func (c *CommandHandlers) HandlerSetCommand() func([]string) {
	return func(parts []string) {
		if len(parts) < 3 {
			fmt.Println("Usage: SET <bucket> <key> <value>")
			return
		}

		bucket := parts[0]
		key, _ := strconv.Atoi(parts[1])
		value := parts[2]

		args := &common.SetArgs{
			Bucket: bucket,
			Key:    key,
			Value:  value,
		}

		var reply string
		err := c.Client.Call("FastDB.Set", args, &reply)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println("Reply from server:", reply)
	}
}

func (c *CommandHandlers) HandlerGetCommand() func([]string) {
	return func(parts []string) {
		if len(parts) < 2 {
			fmt.Println("Usage: GET <bucket> <key>")
			return
		}

		bucket := parts[0]
		key, _ := strconv.Atoi(parts[1])

		args := &common.GetArgs{
			Bucket: bucket,
			Key:    key,
		}
		var reply string
		err := c.Client.Call("FastDB.Get", args, &reply)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println("Reply from server:", reply)
	}
}

func (c *CommandHandlers) HandlerGetAllCommand() func([]string) {
	return func(parts []string) {
		if len(parts) < 1 {
			fmt.Println("Usage: GETALL <bucket>")
			return
		}

		bucket := parts[0]

		args := &common.GetAllArgs{
			Bucket: bucket,
		}
		var reply map[int]string
		err := c.Client.Call("FastDB.GetAll", args, &reply)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		for key, value := range reply {
			fmt.Println("Reply from server: Key: " + strconv.Itoa(key) + ", Value: " + value)
		}
	}
}

func (c *CommandHandlers) HandlerDeleteCommand() func([]string) {
	return func(parts []string) {
		if len(parts) < 2 {
			fmt.Println("Usage: DELETE <bucket> <key>")
			return
		}

		bucket := parts[0]
		key, _ := strconv.Atoi(parts[1])

		args := &common.DeleteArgs{
			Bucket: bucket,
			Key:    key,
		}
		var reply string
		err := c.Client.Call("FastDB.Delete", args, &reply)

		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println("Reply from server:", reply)
	}
}
