// File Sever: server/server.go
package main

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"time"
	"lab02_replication/server/fastdb_rpc/config"
	"lab02_replication/server/replication_rpc/domain"
)

func checkError(err error) {
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func main() {

	go func() {
		node := domain.NewNode()
		
		nodeListener, err := net.Listen("tcp", node.Addr)
		checkError(err)

		rpcServer := rpc.NewServer()
		rpcServer.Register(node)

		go rpcServer.Accept(nodeListener)

		node.RegisterWithPeers();

		warmupTime := 5 * time.Second
		time.Sleep(warmupTime)
		node.TriggerLeaderElection()

	}()

	go func() {
		clientListener, err := net.Listen("tcp", ":1234")
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
			conn, err := clientListener.Accept()
			checkError(err)

			go rpc.ServeConn(conn)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
