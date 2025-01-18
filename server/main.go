// File Sever: server/server.go
package main

import (
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"

	"lab02_replication/server/config"
	"lab02_replication/server/replication_rpc/domain"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	setLogConfigurations()

	rpcServer := rpc.NewServer()

	repository, _ := config.InitializeDB()
	config.RegisterRPCService(rpcServer, repository)

	node := domain.NewNode()
	config.RegisterRPCNodeReplication(rpcServer, node)

	log.Info().Msgf("%s is running on port %s, listening to proxy server at %s", node.ID, node.Addr, "1234")

	nodeListener, _ := net.Listen("tcp", node.Addr)
	go rpcServer.Accept(nodeListener)

	node.StartLeaderElection()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}

func setLogConfigurations() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// https://github.com/rs/zerolog#add-file-and-line-number-to-log
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
