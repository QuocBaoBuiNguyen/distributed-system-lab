package route

import (
	"net/rpc"
	"sync"

	"fmt"
	"lab02_replication/common"
)

type RouteService struct {
	PrimaryNodeClient *rpc.Client
	mu                sync.Mutex
}

func (route *RouteService) PrimaryNodeProxyUpdate(newPrimaryAddr *common.PrimaryNodeProxyUpdateArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	fmt.Println("Proxy: Primary node updated to %s\n", newPrimaryAddr.NewPrimaryAddr)
	route.PrimaryNodeClient.Close()
	route.PrimaryNodeClient, _ = rpc.Dial("tcp", newPrimaryAddr.NewPrimaryAddr)

	return nil
}

func (route *RouteService) Set(args *common.SetArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	route.PrimaryNodeClient.Call("FastDB.Set", args, &reply)

	return nil
}

func (route *RouteService) Get(args *common.GetArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	route.PrimaryNodeClient.Call("FastDB.Get", args, &reply)

	return nil
}

func (route *RouteService) GetAll(args *common.GetAllArgs, reply *map[int]string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	route.PrimaryNodeClient.Call("FastDB.GetAll", args, &reply)

	return nil
}

func (route *RouteService) Delete(args *common.DeleteArgs, reply *string) error {
	route.mu.Lock()
	defer route.mu.Unlock()

	route.PrimaryNodeClient.Call("FastDB.Delete", args, &reply)

	return nil
}
