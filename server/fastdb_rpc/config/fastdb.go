package config

import (
	"github.com/marcelloh/fastdb"
	"lab02_replication/server/fastdb_rpc/utils"
)

func InitializeDB() (*fastdb.DB, error) {
	repository, err := fastdb.Open(":memory:", 100)
	
	utils.CheckError(err)
	
	defer func() {
		err = repository.Close()
		utils.CheckError(err)
	}()

	return repository, err 
}
