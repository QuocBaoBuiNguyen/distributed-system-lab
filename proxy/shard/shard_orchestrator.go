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
	log.Info().Msgf("[Primary Node] - [Event]: Getting shard id by key: %s, shard id: %s, hash: %d", bucket, shardID, so.hash(bucket))
	addr := so.LeaderMapping[shardID]

	return rpc.Dial("tcp", addr)
}

func (so *ShardOrchestrator) GetNodesAddrByShardID(req *common.GetNodesByShardIDArgs, reply *common.GetNodesByShardIDRes) error {
	so.RLock()
	defer so.RUnlock()
	reply.Ports = so.ShardNodesMapping[req.ShardID]

	log.Info().Msgf("[Primary Node] - [Event]: Getting nodes to shard id: %s, ports: %v", req.ShardID, reply.Ports)
	return nil
}

func (so *ShardOrchestrator) RegisterNodeToShard(nodeInfo *common.NodeInfoArgs, reply *string) error {
	so.Lock()
	defer so.Unlock()
	log.Info().Msgf("[Primary Node] - [Event]: Adding nodes by shard id: %s, ports: %v", nodeInfo.ShardID, nodeInfo.Port)
	so.ShardNodesMapping[nodeInfo.ShardID] = append(so.ShardNodesMapping[nodeInfo.ShardID], nodeInfo.Port)
	for _, port := range so.ShardNodesMapping[nodeInfo.ShardID] {
		log.Info().Msgf("[Primary Node] - [Event]: Nodes by shard id: %s, ports: %v", nodeInfo.ShardID, port)

		client, err := rpc.Dial("tcp", port)
		if (client == nil) || (err != nil) {
			log.Fatal().Msgf("Error connecting to RPC server: %v", err)
		}
		defer client.Close()

		args := &common.NotifyNodesInShardArgs{Ports: so.ShardNodesMapping[nodeInfo.ShardID]}
		var reply string

		err = client.Call("ShardServer.NotifyShardPeerUpdate", args, &reply)
		if (client == nil) || (err != nil) {
			log.Info().Msgf("Error connecting to RPC serverhuifdhfasfiausdhfiu: %s", err)
		}
	}

	return nil
}

func (so *ShardOrchestrator) NotifyShardLeaderChange(addr *common.NodeInfoArgs, reply *string) error {
	so.Lock()
	defer so.Unlock()

	if _, exists := so.LeaderMapping[addr.ShardID]; !exists {
		so.migrateDataToNewShard(addr.ShardID, addr.Port)
		so.addShard(addr.ShardID)
		log.Info().Msgf("[Primary Node] - [Event]: Adding new shard succesfully: %s", addr.ShardID)
	}

	so.LeaderMapping[addr.ShardID] = addr.Port
	log.Info().Msgf("[Primary Node] - [Event]: Leader shard mapping: %v", so.LeaderMapping)

	log.Info().Msgf("[Primary Node] - [Event]: Updating proxy server config: %s", addr.Port)
	*reply = "Leader updated successfully"
	return nil
}

func (so *ShardOrchestrator) addShard(shardID string) {
	// so.Lock()
	// defer so.Unlock()

	so.HashRing.Add(shardID)
}

func (so *ShardOrchestrator) migrateDataToNewShard(newShardID, newShardLeaderAddr string) {
	oldShardID, _ := so.findOldShardIDForMigration(newShardID)
	log.Info().Msgf("[Primary Node] - [Event]: Migrating data from shard %s to shard %s", oldShardID, newShardID)
	if shardLeaderAddr, exists := so.LeaderMapping[oldShardID]; exists {

		client, _ := rpc.Dial("tcp", shardLeaderAddr)
		defer client.Close()
		log.Info().Msgf("[Primary Node] - [Event]: Migrating data from shard %s to shard %s, src add: %s , des addr: %s", oldShardID, newShardID, shardLeaderAddr, newShardLeaderAddr)
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
		log.Info().Msgf("Hashes: %v, ShardId: %s", hashes, shardID)
	}

	log.Info().Msgf("Hashes: %v", hashes)

	sort.Slice(hashes, func(i int, j int) bool {
		return hashes[i] < hashes[j]
	})

	// 2023221788722217443
	// 7050743468674691788
	// 15719877831278515272
	// 17008058666120136907

	newHash := so.hash(newShard)
	for i, h := range hashes {
		log.Info().Msgf("sorted Hashes: %v, i: %d, h: %d, newHash: %d", hashes, i, h, newHash)
		if h >= newHash {
			if i == 0 {
				log.Info().Msgf("Start: %d, End: %d", hashes[len(hashes)-1], newHash)
				return common.HashRange{
					Start: hashes[len(hashes)-1],
					End:   newHash, // Nếu là shard đầu tiên, lấy phạm vi từ cuối vòng băm
				}
			}
			log.Info().Msgf("Start: %d, End: %d", hashes[i-1], newHash)
			return common.HashRange{
				Start: hashes[i-1],
				End:   newHash,
			}
		}
	}

	log.Info().Msgf("Start: %d, End: %d", hashes[len(hashes)-1], newHash)

	return common.HashRange{
		Start: hashes[len(hashes)-1],
		End:   newHash,
	}
}

func (c *ShardOrchestrator) hash(key string) uint64 {
	out := blake2b.Sum512([]byte(key))
	return binary.LittleEndian.Uint64(out[:])
}
