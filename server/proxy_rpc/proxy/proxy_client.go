package proxy

import (
	"lab02_replication/common"
	"net/rpc"

	"github.com/rs/zerolog/log"
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

func (p *ProxyServer) NotifyShardLeaderChange(shardID string, port string) error {
	var reply string
	shardLeaderInfo := &common.NodeInfoArgs{
		Port:    port,
		ShardID: shardID,
	}
	log.Info().Msgf("[Primary Node] - [Event]: Updating proxy server config: %s", port)
	err := p.ProxyClient.Call("ShardOrchestrator.NotifyShardLeaderChange", shardLeaderInfo, &reply)
	return err
}

func (p *ProxyServer) GetNodesAddrByShardID(shardID string) []string {
	var ports []string
	shardInfo := &common.GetNodesByShardIDArgs{
		ShardID: shardID,
	}
	log.Info().Msgf("[Primary Node] - [Event]: Getting nodes by shard id: %s", shardID)
	p.ProxyClient.Call("ShardOrchestrator.GetNodesAddrByShardID", shardInfo, ports)
	return ports
}

func (p *ProxyServer) RegisterNodeToShard(port string, shardID string) error {
	var reply *string
	nodeInfo := &common.NodeInfoArgs{
		Port:    port,
		ShardID: shardID,
	}
	log.Info().Msgf("[Primary Node] - [Event]: Registering node to shard: %s", port)
	err := p.ProxyClient.Call("ShardOrchestrator.RegisterNodeToShard", nodeInfo, &reply)
	return err
}
