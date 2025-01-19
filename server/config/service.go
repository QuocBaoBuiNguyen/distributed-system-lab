package config

import (
	"net/rpc"

	"lab02_replication/server/fastdb_rpc/controller"
	"lab02_replication/server/fastdb_rpc/utils"
	"lab02_replication/server/replication_rpc/core"

	"github.com/marcelloh/fastdb"
)

func RegisterRPCService(rpcServer *rpc.Server, repository *fastdb.DB, node *core.Node) error {
	service := controller.NewFastDBService(repository, node);
	err := rpcServer.RegisterName("FastDB", service)
	utils.CheckError(err)
	return err
}

func RegisterRPCNodeReplication(rpcServer *rpc.Server, node *core.Node) error {
	err := rpcServer.RegisterName("Node", node)
	utils.CheckError(err)
	return err
}
