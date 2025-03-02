package core

import (
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	. "lab02_replication/server/proxy_rpc/proxy"
	"lab02_replication/server/replication_rpc/event"
)

type Node struct {
	ShardID  string
	ID       string
	Addr     string
	Peers    *Peers
	Proxy    *ProxyServer
	eventBus event.Bus
	isLeader bool
}

func NewNode() *Node {

	shardID, port := validateAndGetInput()

	node := &Node{
		ID:       getNodeID(port),
		Addr:     getPort(port),
		ShardID:  shardID,
		Peers:    NewPeers(),
		Proxy:    NewProxyServer(),
		eventBus: event.NewBus(),
	}

	node.eventBus.Subscribe(event.LeaderElected, node.HealthCheckLeader)

	return node
}

func (node *Node) HealthCheckLeader(_ string, payload any) {
	leaderID := payload.(string)

ping:
	leader := node.Peers.Get(leaderID)
	if leader == nil {
		log.Error().Msgf("%s, %s, %s", node.ID, leaderID, node.Peers.ToIDs())
		return
	}

	pingMessage := event.Message{FromPeerID: node.ID, Type: event.PING}
	reply, err := node.SendPeerRequest(leader.RPCClient, pingMessage)

	if err != nil {
		log.Info().Msgf("Leader is down, new election about to start!")
		node.Peers.Delete(leaderID)
		node.TriggerLeaderElection()
		return
	}

	if reply.IsPongMessage() {
		log.Debug().Msgf("Leader %s sent PONG message", reply.FromPeerID)
		time.Sleep(3 * time.Second)
		goto ping
	}
}

func (node *Node) RegisterNodeToShard(addr string, shardID string) {
	node.Proxy.RegisterNodeToShard(addr, shardID)
}

func (node *Node) RegisterWithPeers() {
	ports := node.Proxy.GetNodesAddrByShardID(node.ShardID)

	nodeAddressByID := make(map[string]string)

	for _, port := range ports {
		nodeID := fmt.Sprintf("node-%s", port)             // Generate dynamic node ID
		nodeAddressByID[nodeID] = fmt.Sprintf(":%s", port) // Assign port value with ":"
	}

	if len(nodeAddressByID) == 0 {
		return
	} else {
		for peerID, peerAddr := range nodeAddressByID {
			if node.IsItself(peerID) {
				continue
			}

			rpcClient := node.connect(peerAddr)
			pingMessage := event.Message{FromPeerID: node.ID, Type: event.PING}
			reply, _ := node.SendPeerRequest(rpcClient, pingMessage)

			if reply.IsPongMessage() {
				log.Debug().Msgf("%s got pong message from %s", node.ID, peerID)
				node.Peers.Add(peerID, rpcClient)
			}
		}
	}

}

func (node *Node) SendPeerRequest(rpcClient *rpc.Client, message event.Message) (event.Message, error) {
	var reply event.Message

	err := rpcClient.Call("Node.DispatchClusterEvent", message, &reply)

	if err != nil {
		log.Debug().Msgf("Error calling HandleMessage %s", err.Error())
	}

	return reply, err
}

func (node *Node) StartLeaderElection() {
	node.RegisterNodeToShard(node.Addr, node.ShardID)
	node.RegisterWithPeers()
	log.Info().Msgf("%s is aware of own peers %s", node.ID, node.Peers.ToIDs())

	warmupTime := 5 * time.Second
	time.Sleep(warmupTime)
	node.TriggerLeaderElection()
}

func (node *Node) IsRankHigherThan(id string) bool {
	return strings.Compare(node.ID, id) == 1
}

func (node *Node) TriggerLeaderElection() {
	isHighestRankedNodeAvailable := false
	peers := node.Peers.ToList()

	for i := range peers {
		peer := peers[i]
		if node.IsRankHigherThan(peer.ID) {
			continue
		}

		log.Debug().Msgf("%s send ELECTION message to peer %s", node.ID, peer.ID)

		electionMessage := event.Message{FromPeerID: node.ID, Type: event.ELECTION}

		reply, _ := node.SendPeerRequest(peers[i].RPCClient, electionMessage)

		if reply.IsAliveMessage() {
			log.Debug().Msgf("%s has at least 1 highest node", node.ID)
			isHighestRankedNodeAvailable = true
		}
	}

	if !isHighestRankedNodeAvailable {
		leaderID := node.ID
		electedMessage := event.Message{FromPeerID: leaderID, Type: event.ELECTED}
		log.Info().Msgf("%s is a new leader", node.ID)
		node.BroadcastToPeers(electedMessage)
		node.Proxy.NotifyShardLeaderChange(node.ShardID, node.Addr)
		node.isLeader = true
	}
}

func (node *Node) DispatchClusterEvent(message event.Message, reply *event.Message) error {
	reply.FromPeerID = node.ID

	switch message.Type {
	case event.ELECTION:
		reply.Type = event.ALIVE
	case event.ELECTED:
		leaderID := message.FromPeerID
		log.Info().Msgf("Election is done. %s has a new leader %s", node.ID, leaderID)
		node.eventBus.Emit(event.LeaderElected, leaderID)
		node.isLeader = false
		reply.Type = event.OK
	case event.PING:
		reply.Type = event.PONG
	}

	return nil
}

func (node *Node) BroadcastToPeers(message event.Message) {
	peers := node.Peers.ToList()

	for i := range peers {
		peer := peers[i]
		node.SendPeerRequest(peer.RPCClient, message)
	}
}

func (node *Node) connect(peerAddr string) *rpc.Client {
retry:
	client, err := rpc.Dial("tcp", peerAddr)
	if err != nil {
		log.Debug().Msgf("Error dialing rpc dial %s", err.Error())
		time.Sleep(50 * time.Millisecond)
		goto retry
	}
	return client
}

func (node *Node) IsItself(id string) bool {
	return node.ID == id
}

func (node *Node) IsLeader() bool {
	return node.isLeader
}

func getPort(port string) string {
	port = fmt.Sprintf(":%s", port)
	return port
}

func getNodeID(port string) string {
	nodeID := fmt.Sprintf("node-%s", port)
	return nodeID
}

func validateAndGetInput() (string, string) {
	if len(os.Args) < 2 {
		log.Fatal().Msg("shard required")
	}
	shardID := os.Args[1]

	if len(os.Args) < 3 {
		log.Fatal().Msg("port required")
	}
	port := os.Args[2]

	return shardID, port
}
