package config

import (
	"net/rpc"

	"lab02_replication/server/fastdb_rpc/controller"

	"github.com/marcelloh/fastdb"
)

func RegisterRPCService(repository *fastdb.DB) error {
	service := &controller.FastDBService{
		Repository: repository,
	}
	return rpc.RegisterName("FastDB", service)
}
