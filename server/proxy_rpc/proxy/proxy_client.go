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

func (p *ProxyServer) PrimaryNodeProxyUpdate(newPrimaryNodeAddr string) error {
	var reply string
	leaderAddrReq := &common.PrimaryNodeProxyUpdateArgs{
		NewPrimaryAddr: newPrimaryNodeAddr,
	}
	log.Info().Msgf("Calling proxy server from leader")
	err := p.ProxyClient.Call("ProxyServer.PrimaryNodeProxyUpdate", leaderAddrReq, &reply)
	log.Info().Msgf(" Error%s ", err)
	return err
}
