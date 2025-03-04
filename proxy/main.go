// File Sever: server/server.go
package main

import (
	"lab02_replication/proxy/router"
	"lab02_replication/proxy/shard"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
)

func checkError(err error) {
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func main() {

	proxyListener, err := net.Listen("tcp", ":1234")
	checkError(err)

	rpcServer := rpc.NewServer()

	shardOrchestrator := shard.NewShardOrchestrator()

	err = rpcServer.RegisterName("ShardOrchestrator", shardOrchestrator)
	checkError(err)

	routeService := router.NewRouteService(shardOrchestrator)

	err = rpcServer.RegisterName("ProxyServer", routeService)
	checkError(err)

	log.Print("message = [Proxy server] is listening on [port: 1234]")

	go rpcServer.Accept(proxyListener)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
