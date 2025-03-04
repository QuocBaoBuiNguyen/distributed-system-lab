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
	log.Info().Msgf("[Node] - [Event]: NotifyShardLeaderChange: shard: %s, port: %s", shardID, port)

	rpcClient, _ := rpc.Dial("tcp", ":1234")
	err := rpcClient.Call("ShardOrchestrator.NotifyShardLeaderChange", shardLeaderInfo, &reply)	
	return err
}

func (p *ProxyServer) GetNodesAddrByShardID(shardID string) []string {
	var nodesRes common.GetNodesByShardIDRes
	shardInfo := &common.GetNodesByShardIDArgs{
		ShardID: shardID,
	}
	log.Info().Msgf("[Node] - [Event]: GetNodesAddrByShardID: %s", shardID)
	p.ProxyClient.Call("ShardOrchestrator.GetNodesAddrByShardID", shardInfo, &nodesRes)
	return nodesRes.Ports
}

func (p *ProxyServer) RegisterNodeToShard(port string, shardID string) error {
	var reply string
	nodeInfo := &common.NodeInfoArgs{
		Port:    port,
		ShardID: shardID,
	}
	
	log.Info().Msgf("[Node] - [Event]: RegisterNodeToShard: %s", port)

	err := p.ProxyClient.Call("ShardOrchestrator.RegisterNodeToShard", nodeInfo, &reply)
	return err
}
