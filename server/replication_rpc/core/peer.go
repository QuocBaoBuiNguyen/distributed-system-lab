package core

import (
	"net/rpc"
	"sync"
)

type Peer struct {
	ID        string
	Port      string
	RPCClient *rpc.Client
}

type Peers struct {
	*sync.RWMutex
	peerByID map[string]*Peer
}

func NewPeers() *Peers {
	return &Peers{
		RWMutex:  &sync.RWMutex{},
		peerByID: make(map[string]*Peer),
	}
}

func (p *Peers) Add(ID string, port string, RPCClient *rpc.Client) {
	p.Lock()
	defer p.Unlock()

	p.peerByID[ID] = &Peer{ID: ID, Port: port ,RPCClient: RPCClient}
}

func (p *Peers) Get(ID string) *Peer {
	p.RLock()
	defer p.RUnlock()

	val := p.peerByID[ID]
	return val
}

func (p *Peers) Delete(ID string) {
	p.Lock()
	defer p.Unlock()

	delete(p.peerByID, ID)
}

func (p *Peers) ToList() []Peer {
	p.RLock()
	defer p.RUnlock()

	peers := make([]Peer, 0, len(p.peerByID))
	for _, peer := range p.peerByID {
		peers = append(peers, *peer)
	}

	return peers
}

func (p *Peers) ToIDs() []string {
	p.RLock()
	defer p.RUnlock()

	peerIDs := make([]string, 0, len(p.peerByID))
	for _, peer := range p.peerByID {
		peerIDs = append(peerIDs, peer.ID)
	}

	return peerIDs
}
