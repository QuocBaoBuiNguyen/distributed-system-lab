package router

import (
	"lab02_replication/common"
	. "lab02_replication/proxy/shard"

	"github.com/rs/zerolog/log"

	"sync"
)

type RouteService struct {
	ShardOrchestrator *ShardOrchestrator
	mu                sync.Mutex
}

func NewRouteService(shardOrchestrator *ShardOrchestrator) *RouteService {
	return &RouteService{
		ShardOrchestrator: shardOrchestrator,
	}
}

func (route *RouteService) Set(args *common.SetArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	rpcClient, _ := route.ShardOrchestrator.GetShardClientByKey(string(args.Bucket))
	defer rpcClient.Close()

	log.Info().Msgf("[Proxy Server] - [Event]: Routing %s command to primary node", "SET")
	rpcClient.Call("FastDB.Set", args, &reply)

	return nil
}

func (route *RouteService) Get(args *common.GetArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	rpcClient, _ := route.ShardOrchestrator.GetShardClientByKey(string(args.Bucket))
	defer rpcClient.Close()

	log.Info().Msgf("[Proxy Server] - [Event]: Routing %s command to primary node", "GET")
	rpcClient.Call("FastDB.Get", args, &reply)

	return nil
}

func (route *RouteService) GetAll(args *common.GetAllArgs, reply *map[int]string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	rpcClient, _ := route.ShardOrchestrator.GetShardClientByKey(string(args.Bucket))
	defer rpcClient.Close()

	log.Info().Msgf("[Proxy Server] - [Event]: Routing %s command to primary node", "GETALL")
	rpcClient.Call("FastDB.GetAll", args, &reply)

	return nil
}

func (route *RouteService) Delete(args *common.DeleteArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	rpcClient, _ := route.ShardOrchestrator.GetShardClientByKey(string(args.Bucket))
	defer rpcClient.Close()

	log.Info().Msgf("[Proxy Server] - [Event]: Routing %s command to primary node", "DELETE")
	rpcClient.Call("FastDB.Delete", args, &reply)

	return nil
}
