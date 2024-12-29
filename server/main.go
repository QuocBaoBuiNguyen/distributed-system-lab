// File Sever: server/server.go
package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"

	"lab01_rpc/common"

	"github.com/marcelloh/fastdb"
)

type FastDBService struct{
	Store *fastdb.DB
}

type SetArgs struct {
	Bucket string
	Key    int
	Value  interface{}
}

type GetArgs struct {
	Bucket string
	Key    int
}

func (p *FastDBService) Set(args *SetArgs, reply *string) error {
	fmt.Println("Bucket: %s, Key: %s, Value: %s", args.Bucket , args.Key, args.Value)

	bytesVal, err := json.Marshal(args.Value)

	if err != nil {
		*reply = "Error while marshalling request"
		return nil
	}

	p.Store.Set(args.Bucket, args.Key, bytesVal)

	*reply = "Saved successfully"

	return nil
}

func (p *FastDBService) Get(args *GetArgs, reply *string) error {
	fmt.Println("Bucket: %s, Key: %s, Value: %s", args.Bucket , args.Key)

	bytesVal, status := p.Store.Get(args.Bucket, args.Key)

	if !status {
		reply = nil
		return nil
	}

	json := string(bytesVal)
	*reply = json

	fmt.Println("Key: %s, Value: %s", args.Key , json)

	return nil
}

func main() {

	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("Can not create sever because:", err)
	}

	store, err := fastdb.Open(":memory:", 100)

	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = store.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	log.Print("Sever is listening on port 1234")

	gob.Register(common.User{})

	service := &FastDBService{
		Store: store,
	}

	rpc.RegisterName("FastDB", service)

	for {
		// Chấp nhận connection
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
		}

		// Phục vụ lời gọi Client trên một goroutine khác để tiếp tục nhận các lời gọi RPC khác
		go rpc.ServeConn(conn)
	}
}
