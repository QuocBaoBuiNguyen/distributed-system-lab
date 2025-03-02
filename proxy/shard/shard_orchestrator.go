package shard

import (
	"lab02_replication/common"
	"net/rpc"
	"reflect"
	"sort"
	"sync"

	"github.com/lafikl/consistent"
	"github.com/rs/zerolog/log"
)

type ShardOrchestrator struct {
	sync.RWMutex
	LeaderMapping map[string]string
	ShardNodesMapping map[string][]string
	HashRing      *consistent.Consistent
}

func NewShardOrchestrator() *ShardOrchestrator {
	return &ShardOrchestrator{
		LeaderMapping: make(map[string]string),
		HashRing:      consistent.New(),
	}
}

func (so *ShardOrchestrator) GetShardClientByKey(bucket string) (*rpc.Client, error) {
	so.RLock()
	defer so.RUnlock()

	shardID, _ := so.HashRing.Get(bucket)
	addr := so.LeaderMapping[shardID]

	return rpc.Dial("tcp", addr)
}

func (so *ShardOrchestrator) GetNodesAddrByShardID(req *common.GetNodesByShardIDArgs, ports *common.GetNodesByShardIDRes) error {
	so.RLock()
	defer so.RUnlock()
	ports.Ports = so.ShardNodesMapping[req.ShardID]
	log.Info().Msgf("[Primary Node] - [Event]: Getting nodes by shard id: %s, ports: %v", req.ShardID, ports.Ports)
	return nil
}

func (so *ShardOrchestrator) RegisterNodeToShard(nodeInfo *common.NodeInfoArgs, reply *string) error {
	so.Lock()
	defer so.Unlock()

	so.ShardNodesMapping[nodeInfo.ShardID] = append(so.ShardNodesMapping[nodeInfo.ShardID], nodeInfo.Port)
	return nil
}

func (so *ShardOrchestrator) NotifyShardLeaderChange(addr *common.NodeInfoArgs, reply *string) error {
	so.Lock()
	defer so.Unlock()

	if _, exists := so.LeaderMapping[addr.ShardID]; !exists {
		so.addShard(addr.ShardID)
		so.migrateDataToNewShard(addr.ShardID, addr.Port)
	}

	so.LeaderMapping[addr.ShardID] = addr.Port

	return nil
}

func (so *ShardOrchestrator) addShard(shardID string) {
	so.Lock()
	defer so.Unlock()

	so.HashRing.Add(shardID)
}

func (so *ShardOrchestrator) migrateDataToNewShard(newShardID, newShardLeaderAddr string) {
	oldShardID, _ := so.findOldShardIDForMigration(newShardID)

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
	m := so.HashRing
	hashKeyFunc := reflect.ValueOf(m).MethodByName("hashKey")
	for shardID, _ := range so.LeaderMapping {
		h := hashKeyFunc.Call([]reflect.Value{reflect.ValueOf(shardID)})[0].Int()
		hashes = append(hashes, uint64(h))
	}

	sort.Slice(hashes, func(i int, j int) bool {
		if hashes[i] < hashes[j] {
			return true
		}
		return false
	})

	for i, h := range hashes {
		if hashKeyFunc.Call([]reflect.Value{reflect.ValueOf(newShard)})[0].Uint() == h {
			if i == 0 {
				return common.HashRange{
					Start: hashes[len(hashes)-1],
					End:   h, // Nếu là shard đầu tiên, lấy phạm vi từ cuối vòng băm
				}
			}
			return common.HashRange{
				Start: hashes[i-1],
				End:   h,
			}
		}
	}
	return common.HashRange{
		Start: 0,
		End:   0,
	}
}
