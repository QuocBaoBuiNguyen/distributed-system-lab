// File Sever: server/server.go
package main

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"lab02_replication/proxy/route"
	
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
	rpcClient, _ := rpc.Dial("tcp", ":6001")

	routeService := &route.RouteService{
		PrimaryNodeClient: rpcClient,
	}

	err = rpcServer.RegisterName("ProxyServer", routeService)
	checkError(err)

	log.Print("message = [Proxy server] is listening on [port: 1234]")
	
	go rpcServer.Accept(proxyListener)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
