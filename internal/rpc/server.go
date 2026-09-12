package rpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"granite/internal/storage"
	pb "granite/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type RaftProposer interface {
	GetState() (uint64, bool)
	Propose(data []byte) (uint64, uint64, bool)
	HandleRequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	HandleAppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	HandleInstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error)
}

type GRPCServer struct {
	pb.UnimplementedRaftServiceServer

	nodeID     string
	raftNode   RaftProposer
	storageEng *storage.Engine
	server     *grpc.Server
}

func NewGRPCServer(nodeID string, raftNode RaftProposer, storageEng *storage.Engine) *GRPCServer {
	return &GRPCServer{
		nodeID:     nodeID,
		raftNode:   raftNode,
		storageEng: storageEng,
	}
}

func (s *GRPCServer) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc server: failed to listen on %q: %w", addr, err)
	}

	s.server = grpc.NewServer(
		grpc.ChainUnaryInterceptor(loggingInterceptor(s.nodeID)),
	)

	pb.RegisterRaftServiceServer(s.server, s)
	reflection.Register(s.server)

	slog.Info("gRPC server listening", "node", s.nodeID, "addr", addr)
	if err := s.server.Serve(lis); err != nil {
		return fmt.Errorf("grpc server: serve error: %w", err)
	}
	return nil
}

func (s *GRPCServer) Stop() {
	if s.server != nil {
		slog.Info("gRPC server stopping", "node", s.nodeID)
		s.server.GracefulStop()
	}
}

func (s *GRPCServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		ResponderId: s.nodeID,
		Message:     fmt.Sprintf("node %q is alive and ready", s.nodeID),
	}, nil
}

func (s *GRPCServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if s.raftNode == nil {
		return &pb.RequestVoteResponse{Term: req.Term, VoteGranted: false}, nil
	}
	return s.raftNode.HandleRequestVote(ctx, req)
}

func (s *GRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if s.raftNode == nil {
		return &pb.AppendEntriesResponse{Term: req.Term, Success: false}, nil
	}
	return s.raftNode.HandleAppendEntries(ctx, req)
}

func (s *GRPCServer) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if s.raftNode == nil {
		return &pb.InstallSnapshotResponse{Term: req.Term}, nil
	}
	return s.raftNode.HandleInstallSnapshot(ctx, req)
}

func (s *GRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.raftNode == nil {
		if s.storageEng != nil {
			if err := s.storageEng.Put(req.Key, req.Value); err != nil {
				return &pb.PutResponse{Success: false, Error: err.Error()}, nil
			}
			return &pb.PutResponse{Success: true}, nil
		}
		return &pb.PutResponse{Success: false, Error: "node not ready"}, nil
	}

	_, isLeader := s.raftNode.GetState()
	if !isLeader {
		return &pb.PutResponse{
			Success: false,
			Error:   "not leader",
		}, nil
	}

	cmd := encodeCommand(storage.OpPut, req.Key, req.Value)
	idx, _, ok := s.raftNode.Propose(cmd)
	if !ok || idx == 0 {
		return &pb.PutResponse{Success: false, Error: "failed to propose to Raft"}, nil
	}

	start := time.Now()
	for time.Since(start) < 2*time.Second {
		val, err := s.storageEng.Get(req.Key)
		if err == nil && string(val) == string(req.Value) {
			return &pb.PutResponse{Success: true}, nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	return &pb.PutResponse{Success: true}, nil
}

func (s *GRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if s.storageEng == nil {
		return &pb.GetResponse{Found: false, Error: "storage engine not ready"}, nil
	}

	val, err := s.storageEng.Get(req.Key)
	if err != nil {
		return &pb.GetResponse{Found: false}, nil
	}
	return &pb.GetResponse{Value: val, Found: true}, nil
}

func (s *GRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.raftNode == nil {
		if s.storageEng != nil {
			if err := s.storageEng.Delete(req.Key); err != nil {
				return &pb.DeleteResponse{Success: false, Error: err.Error()}, nil
			}
			return &pb.DeleteResponse{Success: true}, nil
		}
		return &pb.DeleteResponse{Success: false, Error: "node not ready"}, nil
	}

	_, isLeader := s.raftNode.GetState()
	if !isLeader {
		return &pb.DeleteResponse{
			Success: false,
			Error:   "not leader",
		}, nil
	}

	cmd := encodeCommand(storage.OpDelete, req.Key, nil)
	idx, _, ok := s.raftNode.Propose(cmd)
	if !ok || idx == 0 {
		return &pb.DeleteResponse{Success: false, Error: "failed to propose delete to Raft"}, nil
	}

	return &pb.DeleteResponse{Success: true}, nil
}

func encodeCommand(op storage.OpType, key, value []byte) []byte {
	buf := make([]byte, 1+2+4+len(key)+len(value))
	buf[0] = byte(op)
	buf[1] = byte(len(key))
	buf[2] = byte(len(key) >> 8)
	buf[3] = byte(len(value))
	buf[4] = byte(len(value) >> 8)
	buf[5] = byte(len(value) >> 16)
	buf[6] = byte(len(value) >> 24)
	copy(buf[7:7+len(key)], key)
	copy(buf[7+len(key):], value)
	return buf
}

func loggingInterceptor(nodeID string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		slog.Debug("RPC received", "node", nodeID, "method", info.FullMethod)
		resp, err := handler(ctx, req)
		if err != nil {
			slog.Error("RPC error", "node", nodeID, "method", info.FullMethod, "error", err)
		}
		return resp, err
	}
}
