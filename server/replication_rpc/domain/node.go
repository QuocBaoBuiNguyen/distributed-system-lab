package domain

import (
	"errors"
	"net/rpc"
	"os"
	"time"

	"lab02_replication/server/replication_rpc/event"
)

var nodeAddressByID = map[string]string{
	"node-01": "node-01:6001",
	"node-02": "node-02:6002",
	"node-03": "node-03:6003",
	"node-04": "node-04:6004",
}

type Node struct {
	ID       string
	Addr     string
	Peers    *Peers
	eventBus event.Bus
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
		// log.Error().Msgf("%s, %s, %s", node.ID, leaderID, node.Peers.ToIDs())
		return
	}

	pingMessage := event.Message{FromPeerID: node.ID, Type: event.PING}
	reply, err := node.SendPeerRequest(leader.rpcClient, pingMessage)

	if err != nil {
	// log.Info().Msgf("Leader is down, new election about to start!")
		node.Peers.Delete(leaderID)
		node.TriggerLeaderElection()
		return
	}

	if reply.IsPongMessage() {
		// log.Debug().Msgf("Leader %s sent PONG message", reply.FromPeerID)
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
			node.Peers.Add(peerID, rpcClient)
		}
	}
}

func (node *Node) SendPeerRequest(rpcClient *rpc.Client, message event.Message) (event.Message, error) {
	var reply event.Message
	
	err := rpcClient.Call("Node.DispatchClusterEvent", message, &reply)
	
	if err != nil {
		// log
	}

	return reply, err
}

func (node *Node) StartLeaderElection() {
	node.RegisterWithPeers()

	warmupTime := 5 * time.Second
	time.Sleep(warmupTime)
	node.TriggerLeaderElection()
}

func (node *Node) TriggerLeaderElection() {
	isHighestRankedNodeAvailable := false
	peers := node.Peers.ToList()

	for i := range peers {
		if peers[i].ID < node.ID {
			continue
		}

		electionMessage := event.Message{FromPeerID: node.ID, Type: event.PING}

		reply, _ := node.SendPeerRequest(peers[i].rpcClient, electionMessage);

		if reply.IsAliveMessage() {
			isHighestRankedNodeAvailable = true
		}
	}
	
	if !isHighestRankedNodeAvailable {
		leaderID := node.ID
		electedMessage := event.Message{FromPeerID: leaderID, Type: event.ELECTED}
		node.BroadcastToPeers(electedMessage)
	}
}

func (node *Node) DispatchClusterEvent(message event.Message, reply *event.Message) error {
	reply.FromPeerID = node.ID

	switch message.Type {
		case event.ELECTION:	
			reply.Type = event.ALIVE
		case event.ELECTED:
			leaderID := message.FromPeerID
			node.eventBus.Emit(event.LeaderElected, leaderID)
			reply.Type = event.OK
		case event.PING:
			reply.Type = event.PONG
	}

	return nil
}

func (node *Node) BroadcastToPeers(message event.Message) {
	peers := node.Peers.ToList();
	
	for i := range peers {
		peer := peers[i]
		node.SendPeerRequest(peer.rpcClient, message)
	}
}

func (node *Node) connect(peerAddr string) *rpc.Client {
	retry:
		client, err := rpc.Dial("tcp", peerAddr)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			goto retry
		}
		return client
	}

func (node *Node) IsItself(id string) bool {
	return node.ID == id
}

