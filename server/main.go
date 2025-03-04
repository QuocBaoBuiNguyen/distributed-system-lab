// File Sever: server/server.go
package main

import (
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"

	"lab02_replication/server/config"
	"lab02_replication/server/replication_rpc/core"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	setLogConfigurations()

	rpcServer := rpc.NewServer()

	node := core.NewNode()
	config.RegisterRPCNodeReplication(rpcServer, node)

	repository, _ := config.InitializeDB()
	fastdb, _ := config.RegisterRPCService(rpcServer, repository, node)

	config.RegisterRPCShardServer(rpcServer, fastdb, node)

	log.Info().Msgf("FastDB Node - [Event]: %s is running on port %s, listening to proxy server at %s", node.ID, node.Addr, "1234")

	nodeListener, _ := net.Listen("tcp", node.Addr)
	go rpcServer.Accept(nodeListener)

	node.StartLeaderElection()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func setLogConfigurations() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		file = short
		return file + ":" + strconv.Itoa(line)
	}

	log.Logger = log.With().Caller().Logger()
}
