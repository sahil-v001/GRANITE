package rpc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "granite/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultRPCTimeout = 200 * time.Millisecond

type peerConn struct {
	peerID string
	conn   *grpc.ClientConn
	client pb.RaftServiceClient
}

type PeerPool struct {
	mu     sync.RWMutex
	selfID string
	peers  map[string]*peerConn
}

func NewPeerPool(selfID string) *PeerPool {
	return &PeerPool{
		selfID: selfID,
		peers:  make(map[string]*peerConn),
	}
}

func (p *PeerPool) Connect(peerID, addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.peers[peerID]; exists {
		return nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("peer pool: failed to dial %q (%s): %w", peerID, addr, err)
	}

	p.peers[peerID] = &peerConn{
		peerID: peerID,
		conn:   conn,
		client: pb.NewRaftServiceClient(conn),
	}

	slog.Info("peer connected", "self", p.selfID, "peer", peerID, "addr", addr)
	return nil
}

func (p *PeerPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for id, peer := range p.peers {
		if err := peer.conn.Close(); err != nil {
			slog.Warn("failed to close peer connection", "peer", id, "error", err)
		}
	}
	p.peers = make(map[string]*peerConn)
}

func (p *PeerPool) GetClientStub(peerID string) (pb.RaftServiceClient, error) {
	peer, err := p.getPeer(peerID)
	if err != nil {
		return nil, err
	}
	return peer.client, nil
}

func (p *PeerPool) Ping(ctx context.Context, peerID string, req *pb.PingRequest) (*pb.PingResponse, error) {
	peer, err := p.getPeer(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	return peer.client.Ping(ctx, req)
}

func (p *PeerPool) Put(ctx context.Context, peerID string, req *pb.PutRequest) (*pb.PutResponse, error) {
	peer, err := p.getPeer(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return peer.client.Put(ctx, req)
}

func (p *PeerPool) Get(ctx context.Context, peerID string, req *pb.GetRequest) (*pb.GetResponse, error) {
	peer, err := p.getPeer(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	return peer.client.Get(ctx, req)
}

func (p *PeerPool) Delete(ctx context.Context, peerID string, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	peer, err := p.getPeer(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return peer.client.Delete(ctx, req)
}

func (p *PeerPool) RequestVote(ctx context.Context, peerID string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	peer, err := p.getPeer(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	return peer.client.RequestVote(ctx, req)
}

func (p *PeerPool) AppendEntries(ctx context.Context, peerID string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	peer, err := p.getPeer(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	return peer.client.AppendEntries(ctx, req)
}

func (p *PeerPool) InstallSnapshot(ctx context.Context, peerID string, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	peer, err := p.getPeer(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return peer.client.InstallSnapshot(ctx, req)
}

func (p *PeerPool) BroadcastPing(ctx context.Context, req *pb.PingRequest) map[string]*PingResult {
	p.mu.RLock()
	peerIDs := make([]string, 0, len(p.peers))
	for id := range p.peers {
		peerIDs = append(peerIDs, id)
	}
	p.mu.RUnlock()

	resultCh := make(chan *PingResult, len(peerIDs))
	var wg sync.WaitGroup

	for _, peerID := range peerIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			resp, err := p.Ping(ctx, id, req)
			resultCh <- &PingResult{PeerID: id, Response: resp, Err: err}
		}(peerID)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	results := make(map[string]*PingResult)
	for r := range resultCh {
		results[r.PeerID] = r
	}
	return results
}

func (p *PeerPool) PeerIDs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ids := make([]string, 0, len(p.peers))
	for id := range p.peers {
		ids = append(ids, id)
	}
	return ids
}

func (p *PeerPool) getPeer(peerID string) (*peerConn, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peer, ok := p.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("peer pool: unknown peer %q (not connected)", peerID)
	}
	return peer, nil
}

type PingResult struct {
	PeerID   string
	Response *pb.PingResponse
	Err      error
}
