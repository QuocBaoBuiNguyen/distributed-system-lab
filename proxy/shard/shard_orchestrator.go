package shard

import (
	"encoding/binary"
	"lab02_replication/common"
	"net/rpc"
	"sort"
	"sync"

	"github.com/lafikl/consistent"
	blake2b "github.com/minio/blake2b-simd"
	"github.com/rs/zerolog/log"
)

type ShardOrchestrator struct {
	sync.RWMutex
	LeaderMapping     map[string]string
	ShardNodesMapping map[string][]string
	HashRing          *consistent.Consistent
}

func NewShardOrchestrator() *ShardOrchestrator {
	return &ShardOrchestrator{
		LeaderMapping:     make(map[string]string),
		ShardNodesMapping: make(map[string][]string),
		HashRing:          consistent.New(),
	}
}

func (so *ShardOrchestrator) GetShardClientByKey(bucket string) (*rpc.Client, error) {
	so.RLock()
	defer so.RUnlock()

	shardID, _ := so.HashRing.Get(bucket)
	log.Info().Msgf("[ShardOrchestrator] - [Event]: GetShardClientByKey: %s, shard id: %s, hash: %d", bucket, shardID, so.hash(bucket))
	addr := so.LeaderMapping[shardID]

	return rpc.Dial("tcp", addr)
}

func (so *ShardOrchestrator) GetNodesAddrByShardID(req *common.GetNodesByShardIDArgs, reply *common.GetNodesByShardIDRes) error {
	so.RLock()
	defer so.RUnlock()
	reply.Ports = so.ShardNodesMapping[req.ShardID]

	log.Info().Msgf("[ShardOrchestrator] - [Event]: GetNodesAddrByShardID: %s, ports: %v", req.ShardID, reply.Ports)
	return nil
}

func (so *ShardOrchestrator) RegisterNodeToShard(nodeInfo *common.NodeInfoArgs, reply *string) error {
	so.Lock()
	defer so.Unlock()
	
	log.Info().Msgf("[ShardOrchestrator] - [Event]: Adding nodes by shard id: %s, ports: %v", nodeInfo.ShardID, nodeInfo.Port)
	
	so.ShardNodesMapping[nodeInfo.ShardID] = append(so.ShardNodesMapping[nodeInfo.ShardID], nodeInfo.Port)
	for _, port := range so.ShardNodesMapping[nodeInfo.ShardID] {
		client, err := rpc.Dial("tcp", port)
		if (client == nil) || (err != nil) {
			log.Fatal().Msgf("Error connecting to RPC server: %v", err)
		}
		defer client.Close()

		args := &common.NotifyNodesInShardArgs{Ports: so.ShardNodesMapping[nodeInfo.ShardID]}
		var reply string
		client.Call("ShardServer.NotifyShardPeerUpdate", args, &reply)
	}

	return nil
}

func (so *ShardOrchestrator) NotifyShardLeaderChange(addr *common.NodeInfoArgs, reply *string) error {
	so.Lock()
	defer so.Unlock()

	if _, exists := so.LeaderMapping[addr.ShardID]; !exists {
		so.migrateDataToNewShard(addr.ShardID, addr.Port)
		so.addShard(addr.ShardID)
	}

	so.LeaderMapping[addr.ShardID] = addr.Port
	log.Info().Msgf("[ShardOrchestrator] - [Event]: NotifyShardLeaderChange succesfully. Leader-ShardID mapping %v", so.LeaderMapping)

	*reply = "Leader updated successfully"
	return nil
}

func (so *ShardOrchestrator) addShard(shardID string) {
	so.HashRing.Add(shardID)
}

func (so *ShardOrchestrator) migrateDataToNewShard(newShardID, newShardLeaderAddr string) {
	oldShardID, _ := so.findOldShardIDForMigration(newShardID)
	log.Info().Msgf("[ShardOrchestrator] - [Event]: Migrating data from %s to %s", oldShardID, newShardID)
	if shardLeaderAddr, exists := so.LeaderMapping[oldShardID]; exists {

		client, _ := rpc.Dial("tcp", shardLeaderAddr)
		defer client.Close()
		args := &common.MigrationRequestArgs{TargetShard: newShardID, TargetAddress: newShardLeaderAddr, HashRange: so.getHashRange(newShardID)}
		var reply string
		client.Call("ShardServer.MigrateData", args, &reply)
	}
}

func (so *ShardOrchestrator) findOldShardIDForMigration(newShardID string) (string, error) {
	return so.HashRing.Get(newShardID)
}

func (so *ShardOrchestrator) getHashRange(newShard string) common.HashRange {
	hashes := []uint64{}

	for shardID, _ := range so.LeaderMapping {
		h := so.hash(shardID)
		hashes = append(hashes, uint64(h))
	}

	sort.Slice(hashes, func(i int, j int) bool {
		return hashes[i] < hashes[j]
	})

	newHash := so.hash(newShard)
	for i, h := range hashes {
		if h >= newHash {
			if i == 0 {
				return common.HashRange{
					Start: hashes[len(hashes)-1],
					End:   newHash,
				}
			}
			return common.HashRange{
				Start: hashes[i-1],
				End:   newHash,
			}
		}
	}

	return common.HashRange{
		Start: hashes[len(hashes)-1],
		End:   newHash,
	}
}

func (c *ShardOrchestrator) hash(key string) uint64 {
	out := blake2b.Sum512([]byte(key))
	return binary.LittleEndian.Uint64(out[:])
}
