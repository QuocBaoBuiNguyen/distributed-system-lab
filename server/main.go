// File Sever: server/server.go
package main

import (
	"lab02_replication/server/fastdb_rpc/config"
	"lab02_replication/server/replication_rpc/domain"
	"net"
	"net/rpc"
	"os"
	"os/signal"
)

func main() {

	rpcServer := rpc.NewServer()

	repository, _ := config.InitializeDB()
	config.RegisterRPCService(rpcServer, repository)

	node := domain.NewNode()
	config.RegisterRPCNodeReplication(rpcServer, node)

	nodeListener, _ := net.Listen("tcp", node.Addr)
	go rpcServer.Accept(nodeListener)

	node.StartLeaderElection()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
