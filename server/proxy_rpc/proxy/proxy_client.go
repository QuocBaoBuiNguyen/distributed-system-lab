package proxy

import (
	"lab02_replication/common"
	"github.com/rs/zerolog/log"
	"net/rpc"
)

type ProxyServer struct {
	ProxyClient *rpc.Client
}

func NewProxyServer() *ProxyServer {
	client, _ := rpc.Dial("tcp", ":1234")
	return &ProxyServer{
		ProxyClient: client,
	}
}

func (p *ProxyServer) PrimaryNodeProxyUpdate(port string) error {
	var reply string
	leaderAddrReq := &common.PrimaryNodeProxyUpdateArgs{
		Port: port,
	}
	log.Info().Msgf("Proxy Server - [Event]: Routing command to primary node %s", port)
	err := p.ProxyClient.Call("ProxyServer.PrimaryNodeProxyUpdate", leaderAddrReq, &reply)
	return err
}
