// File Sever: server/server.go
package main

import (
	"encoding/json"
	"fmt"

	"log"
	"net"
	"net/rpc"
	"strconv"

	. "lab01_rpc/common"

	"github.com/marcelloh/fastdb"
)

type FastDBService struct {
	Store *fastdb.DB
}

func (p *FastDBService) Set(args *SetArgs, reply *string) error {
	fmt.Println("Set: Bucket: %s, Key: %s, Value: %s.", args.Bucket, args.Key, args.Value)

	dbRecord, err := json.Marshal(args.Value)

	if err != nil {
		*reply = "Error while marshalling request"
		return nil
	}

	p.Store.Set(args.Bucket, args.Key, dbRecord)

	*reply = "Saved successfully"

	return nil
}

func (p *FastDBService) Get(args *GetArgs, reply *string) error {
	fmt.Println("Get: Bucket: %s, Key: %s", args.Bucket, args.Key)

	dbRecord, status := p.Store.Get(args.Bucket, args.Key)

	if !status {
		reply = nil
		return nil
	}

	json := string(dbRecord)
	*reply = json

	fmt.Println("Key: %s, Value: %s", args.Key, json)

	return nil
}

func (p *FastDBService) GetAll(args *GetAllArgs, reply *map[int]string) error {
	fmt.Println("Get All: Bucket: %s", args.Bucket)

	dbRecords, err := p.Store.GetAll(args.Bucket)

	if err != nil {
		return fmt.Errorf("bucket not found: %s", args.Bucket)
	}

	result := make(map[int]string)
	for key, record := range dbRecords {
		result[key] = string(record)
	}

	*reply = result

	return nil
}

func (p *FastDBService) Delete(args *DeleteArgs, reply *string) error {
	fmt.Println("Delete: Bucket: %s.", args.Bucket)

	isDeleted, err := p.Store.Del(args.Bucket, args.Key)

	if err != nil {
		return fmt.Errorf("bucket not found: %s", args.Bucket)
	}

	if isDeleted {
		*reply = "Deleted ele own this key: " + strconv.Itoa(args.Key) + " successfully."
		return nil
	}
	*reply = "Error occured while deleting this object."
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

	service := &FastDBService{
		Store: store,
	}

	rpc.RegisterName("FastDB", service)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
		}

		go rpc.ServeConn(conn)
	}
}
