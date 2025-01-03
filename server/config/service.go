package config

import (
	"net/rpc"

	"lab01_rpc/server/controller"

	"github.com/marcelloh/fastdb"
)

func RegisterRPCService(repository *fastdb.DB) error {
	service := &controller.FastDBService{
		Repository: repository,
	}
	return rpc.RegisterName("FastDB", service)
}
