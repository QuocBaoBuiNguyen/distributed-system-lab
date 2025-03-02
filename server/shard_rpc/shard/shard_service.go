package shard

import (
	"net/rpc"

	"lab02_replication/common"
	"lab02_replication/server/fastdb_rpc/controller"
)

type ShardService struct {
	// Add necessary fields here
	FastDBService *controller.FastDBService
}

func NewShardService(fastDBService *controller.FastDBService) *ShardService {
	return &ShardService{
		// Initialize fields here
		FastDBService: fastDBService,
	}
}

func (s *ShardService) MigrateData(req *common.MigrationRequestArgs, reply *string) error {

	buckets := s.FastDBService.GetAllBucketsInRange(req.HashRange.Start, req.HashRange.End)

	rpcClient, _ := rpc.Dial("tcp", req.TargetAddress)
	defer rpcClient.Close()

	for _, bucket := range buckets {
		var resGetAll map[int]string
		s.FastDBService.GetAll(&common.GetAllArgs{Bucket: bucket}, &resGetAll)

		for key, value := range resGetAll {
			var resSet string
			rpcClient.Call("FastDB.Set", &common.SetArgs{Bucket: bucket, Key: key, Value: value}, &resSet)
			if resSet == "Saved successfully" {
				s.FastDBService.Delete(&common.DeleteArgs{Bucket: bucket, Key: key}, reply)
			}
		}
	}

	*reply = "Data migration successful"

	return nil
}
