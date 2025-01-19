package router

import (
	"lab02_replication/common"

	"github.com/rs/zerolog/log"

	"net/rpc"
	"sync"
)

type RouteService struct {
	PrimaryNodeClient *rpc.Client
	mu                sync.Mutex
}

func (route *RouteService) PrimaryNodeProxyUpdate(addr *common.PrimaryNodeProxyUpdateArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	if route.PrimaryNodeClient != nil {
		route.PrimaryNodeClient.Close()
	}

	route.PrimaryNodeClient, _ = rpc.Dial("tcp", addr.Port)
	log.Info().Msgf("[Proxy Server] - [Event]: new primary node has been promoted, updated listener port: %s", addr.Port)

	return nil
}

func (route *RouteService) Set(args *common.SetArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	log.Info().Msgf("[Proxy Server] - [Event]: Routing %s command to primary node", "SET")
	route.PrimaryNodeClient.Call("FastDB.Set", args, &reply)

	return nil
}

func (route *RouteService) Get(args *common.GetArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	log.Info().Msgf("[Proxy Server] - [Event]: Routing %s command to primary node", "GET")
	route.PrimaryNodeClient.Call("FastDB.Get", args, &reply)

	return nil
}

func (route *RouteService) GetAll(args *common.GetAllArgs, reply *map[int]string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	log.Info().Msgf("[Proxy Server] - [Event]: Routing %s command to primary node", "GETALL")
	route.PrimaryNodeClient.Call("FastDB.GetAll", args, &reply)

	return nil
}

func (route *RouteService) Delete(args *common.DeleteArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	log.Info().Msgf("[Proxy Server] - [Event]: Routing %s command to primary node", "DELETE")
	route.PrimaryNodeClient.Call("FastDB.Delete", args, &reply)

	return nil
}
