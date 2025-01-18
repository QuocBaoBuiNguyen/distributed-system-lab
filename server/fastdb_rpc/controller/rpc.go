package controller

import (
	"encoding/json"
	"lab02_replication/common"
	"lab02_replication/server/fastdb_rpc/operation"
	"lab02_replication/server/replication_rpc/domain"
	"log"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"github.com/marcelloh/fastdb"
)

type FastDBService struct {
	Repository *fastdb.DB
	Node       *domain.Node
	mu         sync.RWMutex
}

func NewFastDBService(repository *fastdb.DB, node *domain.Node) *FastDBService {
	service := &FastDBService{
		Repository: repository,
		Node:       node,
	}

	return service
}

func (p *FastDBService) Set(args *common.SetArgs, reply *string) error {
	start := time.Now()
	log.Printf("[SET] Starting for key: %d at %s\n", args.Key, start.Format(time.StampMilli))

	p.mu.Lock()
	defer p.mu.Unlock()

	log.Printf("SET: Bucket=%s, Key=%d, Value=%s, Status=processing", args.Bucket, args.Key, args.Value)

	dbRecord, err := json.Marshal(args.Value)
	if err != nil {
		*reply = "Error while marshalling request"
		log.Printf("SET: Bucket=%s, Key=%d, Value=%s, Status=failure, Error=%v", args.Bucket, args.Key, args.Value, err)
		return err
	}

	p.Repository.Set(args.Bucket, args.Key, dbRecord)
	log.Printf("SET: Bucket=%s, Key=%d, Value=%s, Status=success", args.Bucket, args.Key, args.Value)
	*reply = "Saved successfully"

	log.Printf("[SET] Completed for key: %d at %s (Duration: %v)\n", args.Key, time.Now().Format(time.StampMilli), time.Since(start))

	if p.Node.IsLeader() {
		op := &operation.Operation{
			RequestBody: args,
			Type:        operation.SET,
		}
		p.BroadcastOperationToPeers(op)
		log.Printf("[SET]: Broadcasted SET Operation To Other Peers")
	}

	return nil
}

func (p *FastDBService) Get(args *common.GetArgs, reply *string) error {
	start := time.Now()
	log.Printf("[GET] Starting for key: %d at %s\n", args.Key, start.Format(time.StampMilli))

	p.mu.RLock()
	defer p.mu.RUnlock()

	log.Printf("GET: Bucket=%s, Key=%d, Status=processing", args.Bucket, args.Key)

	dbRecord, status := p.Repository.Get(args.Bucket, args.Key)
	if !status {
		*reply = "Key not found"
		log.Printf("GET: Bucket=%s, Key=%d, Status=not_found", args.Bucket, args.Key)
		return nil
	}

	*reply = string(dbRecord)
	log.Printf("GET: Bucket=%s, Key=%d, Status=success, Value=%s", args.Bucket, args.Key, *reply)

	log.Printf("[GET] Completed for key: %d at %s (Duration: %v)\n", args.Key, time.Now().Format(time.StampMilli), time.Since(start))

	return nil
}

func (p *FastDBService) GetAll(args *common.GetAllArgs, reply *map[int]string) error {
	start := time.Now()
	log.Printf("[GET ALL] Starting for bucket: %s at %s\n", args.Bucket, start.Format(time.StampMilli))

	p.mu.RLock()
	defer p.mu.RUnlock()

	log.Printf("GET ALL: Bucket=%s, Status=processing", args.Bucket)

	dbRecords, err := p.Repository.GetAll(args.Bucket)
	if err != nil {
		log.Printf("GET ALL: Bucket=%s, Status=failure, Error=%v", args.Bucket, err)
		return err
	}

	if len(dbRecords) == 0 {
		log.Printf("GET ALL: Bucket=%s, Status=not_found", args.Bucket)
		return nil
	}

	result := make(map[int]string)
	for key, record := range dbRecords {
		result[key] = string(record)
	}

	*reply = result
	log.Printf("GET ALL: Bucket=%s, Status=success, RecordCount=%d", args.Bucket, len(dbRecords))

	log.Printf("[GET ALL] Completed for bucket: %s at %s (Duration: %v)\n", args.Bucket, time.Now().Format(time.StampMilli), time.Since(start))

	return nil
}

func (p *FastDBService) Delete(args *common.DeleteArgs, reply *string) error {
	start := time.Now()
	log.Printf("[DELETE] Starting for key: %d at %s\n", args.Key, start.Format(time.StampMilli))

	p.mu.Lock()
	defer p.mu.Unlock()

	log.Printf("DELETE: Bucket=%s, Key=%d, Status=processing", args.Bucket, args.Key)

	isDeleted, err := p.Repository.Del(args.Bucket, args.Key)

	if err != nil {
		log.Printf("DELETE: Bucket=%s, Key=%d, Status=failure, Error=%v", args.Bucket, args.Key, err)
		return err
	}

	if isDeleted {
		*reply = "Deleted entry (which owns key: " + strconv.Itoa(args.Key) + ") successfully."
		log.Printf("DELETE: Bucket=%s, Key=%d, Status=success", args.Bucket, args.Key)

		log.Printf("[DELETE] Completed for key: %d at %s (Duration: %v)\n", args.Key, time.Now().Format(time.StampMilli), time.Since(start))

		return nil
	}

	*reply = "Key not found."
	log.Printf("DELETE: Bucket=%s, Key=%d, Status=not_found", args.Bucket, args.Key)

	if p.Node.IsLeader() {
		op := &operation.Operation{
			RequestBody: args,
			Type:        operation.DELETE,
		}
		p.BroadcastOperationToPeers(op)
		log.Printf("[DELETE]: Broadcasted DELETE Operation To Other Peers")
	}

	return nil
}

func (p *FastDBService) BroadcastOperationToPeers(operation *operation.Operation) {
	peers := p.Node.Peers.ToList()

	for i := range peers {
		peer := peers[i]
		p.OperationHandler(peer.RPCClient, operation)
	}

}

func (p *FastDBService) OperationHandler(rpcClient *rpc.Client, op *operation.Operation) {
	switch op.Type {
	case operation.SET:
		var reply string
		rpcClient.Call("FastDB.Set", op.RequestBody, &reply)
	case operation.DELETE:
		var reply string
		rpcClient.Call("FastDB.Delete", op.RequestBody, &reply)

	}
}
