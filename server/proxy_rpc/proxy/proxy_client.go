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
	client, err := rpc.Dial("tcp", ":1234")
	if (client == nil) || (err != nil) {
		log.Fatal().Msgf("Error connecting to RPC server: %v", err)
	}
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
	rpcClient, _ := rpc.Dial("tcp", ":1234")
	err := rpcClient.Call("ShardOrchestrator.NotifyShardLeaderChange", shardLeaderInfo, &reply)
	if err != nil {
		log.Error().Msgf("hahah %s", err)
	}
	log.Error().Msgf("lolololl %s", err)
	
	return err
}

func (p *ProxyServer) GetNodesAddrByShardID(shardID string) []string {
	var nodesRes common.GetNodesByShardIDRes
	shardInfo := &common.GetNodesByShardIDArgs{
		ShardID: shardID,
	}
	log.Info().Msgf("[Primary Node] - [Event]: Getting nodes by shard id: %s", shardID)
	p.ProxyClient.Call("ShardOrchestrator.GetNodesAddrByShardID", shardInfo, &nodesRes)
	return nodesRes.Ports
}

func (p *ProxyServer) RegisterNodeToShard(port string, shardID string) error {
	var reply string
	nodeInfo := &common.NodeInfoArgs{
		Port:    port,
		ShardID: shardID,
	}
	log.Info().Msgf("[Primary Node] - [Event]: Registering node to shard: %s", port)
	err := p.ProxyClient.Call("ShardOrchestrator.RegisterNodeToShard", nodeInfo, &reply)
	if err != nil {
		log.Fatal().Msgf("Error registering node to shard: %v", err)
	}
	log.Info().Msgf("[Primary Node] - [Event]: Node registered to shard: %s", err)
	return err
}
