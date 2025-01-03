// File Sever: server/server.go
package main

import (
	"log"
	"net"
	"net/rpc"

	"lab01_rpc/server/config"
)

func checkError(err error) {
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func main() {

	listener, err := net.Listen("tcp", ":1234")
	checkError(err)

	repository, err := config.InitializeDB()
	checkError(err)

	err = config.RegisterRPCService(repository)
	checkError(err)

	defer func() {
		err = repository.Close()
		checkError(err)
	}()

	log.Print("message = Sever is listening on port 1234.")

	for {
		conn, err := listener.Accept()
		checkError(err)

		go rpc.ServeConn(conn)
	}
}
