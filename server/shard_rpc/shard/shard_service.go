package shard

import (
	"net/rpc"
	"slices"
	"strings"

	"lab02_replication/common"
	"lab02_replication/server/fastdb_rpc/controller"
	"lab02_replication/server/replication_rpc/core"

	"github.com/rs/zerolog/log"
)

type ShardService struct {
	// Add necessary fields here
	FastDBService *controller.FastDBService
	Node          *core.Node
}

func NewShardService(fastDBService *controller.FastDBService, node *core.Node) *ShardService {
	return &ShardService{
		// Initialize fields here
		FastDBService: fastDBService,
		Node:          node,
	}
}

func (s *ShardService) MigrateData(req *common.MigrationRequestArgs, reply *string) error {

	buckets := s.FastDBService.GetAllBucketsInRange(req.HashRange.Start, req.HashRange.End)

	log.Info().Msgf("[ShardService]: Migrating data from buckets %v to %s with hash range %v (start: %d, end: %d)", buckets, req.TargetAddress, req.HashRange, req.HashRange.Start, req.HashRange.End)

	rpcClient, _ := rpc.Dial("tcp", req.TargetAddress)
	defer rpcClient.Close()

	for _, bucket := range buckets {
		var resGetAll map[int]string
		s.FastDBService.GetAll(&common.GetAllArgs{Bucket: bucket}, &resGetAll)

		log.Info().Msgf("[ShardService]: Migrated data from bucket, target: %s, bucket: %s", req.TargetAddress, bucket)

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

func (s *ShardService) NotifyShardPeerUpdate(req *common.NotifyNodesInShardArgs, res *string) error {

	log.Info().Msgf("[ShardService]: Received update peers")

	for _, port := range req.Ports {
		nodeID := s.Node.GetNodeID(strings.TrimPrefix(port, ":"))
		if !slices.Contains(s.Node.Peers.ToIDs(), nodeID) && nodeID != s.Node.ID {
			s.Node.Peers.Add(nodeID, port, s.Node.Connect(port))
		}
	}
	log.Info().Msgf("[ShardService]: Updated peers %s", s.Node.Peers.ToIDs())
	return nil
}
