// File Client: client/main.go
package main

import (
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"log"
	"net/rpc"
	"strconv"

	"lab01_rpc/common"
)

type SetArgs struct {
	Bucket string
	Key    int
	Value  interface{}
}

type GetArgs struct {
	Bucket string
	Key    int
}

func main() {
	client, err := rpc.Dial("tcp", "localhost:1234")

	if err != nil {
		log.Fatal(err)
	}

	gob.Register(common.User{})

	var reply string
	user := &common.User{
		ID:    1,
		UUID:  "UUIDtext_",
		Email: "test@example.com",
	}
	user.UUID = "UUIDtext_" + generateRandomString(8) + strconv.Itoa(user.ID)

	if err != nil {
		log.Fatal(err)
	}

	args := &SetArgs{
		Bucket: "user_bucket",
		Key:    user.ID,
		Value:  user,
	}

	err = client.Call("FastDB.Set", args, &reply)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(reply)

	getArgs := &GetArgs{
		Bucket: "user_bucket",
		Key:    user.ID,
	}

	err = client.Call("FastDB.Get", getArgs, &reply)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(reply)
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	b := make([]byte, length)

	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}

	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}

	return string(b)
}
