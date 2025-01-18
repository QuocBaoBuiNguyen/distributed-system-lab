package domain

import (
	"errors"
	"net/rpc"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	. "lab02_replication/server/proxy_rpc/proxy"
	"lab02_replication/server/replication_rpc/event"
)

var nodeAddressByID = map[string]string{
	"node-01": ":6001",
	"node-02": ":6002",
	"node-03": ":6003",
	"node-04": ":6004",
}

type Node struct {
	ID       string
	Addr     string
	Peers    *Peers
	Proxy    *ProxyServer
	eventBus event.Bus
	isLeader bool
}

func getNodeID() (string, error) {
	if len(os.Args) < 2 {
		return "", errors.New("node id required")
	}

	nodeID := os.Args[1]
	return nodeID, nil
}

func NewNode() *Node {
	nodeID, _ := getNodeID()

	node := &Node{
		ID:       nodeID,
		Addr:     nodeAddressByID[nodeID],
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

func (node *Node) RegisterWithPeers() {
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

func (node *Node) SendPeerRequest(rpcClient *rpc.Client, message event.Message) (event.Message, error) {
	var reply event.Message

	err := rpcClient.Call("Node.DispatchClusterEvent", message, &reply)

	if err != nil {
		log.Debug().Msgf("Error calling HandleMessage %s", err.Error())
	}

	return reply, err
}

func (node *Node) StartLeaderElection() {
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
		node.Proxy.PrimaryNodeProxyUpdate(node.Addr)
		node.isLeader = true;
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
	return node.isLeader;
}