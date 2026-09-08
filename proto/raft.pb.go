package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)

	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type LogEntry struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	Index uint64 `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`

	Term uint64 `protobuf:"varint,2,opt,name=term,proto3" json:"term,omitempty"`

	Data          []byte `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LogEntry) Reset() {
	*x = LogEntry{}
	mi := &file_proto_raft_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LogEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogEntry) ProtoMessage() {}

func (x *LogEntry) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LogEntry) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{0}
}

func (x *LogEntry) GetIndex() uint64 {
	if x != nil {
		return x.Index
	}
	return 0
}

func (x *LogEntry) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *LogEntry) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type RequestVoteRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	Term uint64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`

	CandidateId string `protobuf:"bytes,2,opt,name=candidate_id,json=candidateId,proto3" json:"candidate_id,omitempty"`

	LastLogIndex uint64 `protobuf:"varint,3,opt,name=last_log_index,json=lastLogIndex,proto3" json:"last_log_index,omitempty"`

	LastLogTerm   uint64 `protobuf:"varint,4,opt,name=last_log_term,json=lastLogTerm,proto3" json:"last_log_term,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RequestVoteRequest) Reset() {
	*x = RequestVoteRequest{}
	mi := &file_proto_raft_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RequestVoteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteRequest) ProtoMessage() {}

func (x *RequestVoteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*RequestVoteRequest) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{1}
}

func (x *RequestVoteRequest) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteRequest) GetCandidateId() string {
	if x != nil {
		return x.CandidateId
	}
	return ""
}

func (x *RequestVoteRequest) GetLastLogIndex() uint64 {
	if x != nil {
		return x.LastLogIndex
	}
	return 0
}

func (x *RequestVoteRequest) GetLastLogTerm() uint64 {
	if x != nil {
		return x.LastLogTerm
	}
	return 0
}

type RequestVoteResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	Term uint64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`

	VoteGranted   bool `protobuf:"varint,2,opt,name=vote_granted,json=voteGranted,proto3" json:"vote_granted,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RequestVoteResponse) Reset() {
	*x = RequestVoteResponse{}
	mi := &file_proto_raft_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RequestVoteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteResponse) ProtoMessage() {}

func (x *RequestVoteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*RequestVoteResponse) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{2}
}

func (x *RequestVoteResponse) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteResponse) GetVoteGranted() bool {
	if x != nil {
		return x.VoteGranted
	}
	return false
}

type AppendEntriesRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	Term uint64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`

	LeaderId string `protobuf:"bytes,2,opt,name=leader_id,json=leaderId,proto3" json:"leader_id,omitempty"`

	PrevLogIndex uint64 `protobuf:"varint,3,opt,name=prev_log_index,json=prevLogIndex,proto3" json:"prev_log_index,omitempty"`

	PrevLogTerm uint64 `protobuf:"varint,4,opt,name=prev_log_term,json=prevLogTerm,proto3" json:"prev_log_term,omitempty"`

	Entries []*LogEntry `protobuf:"bytes,5,rep,name=entries,proto3" json:"entries,omitempty"`

	LeaderCommit  uint64 `protobuf:"varint,6,opt,name=leader_commit,json=leaderCommit,proto3" json:"leader_commit,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *AppendEntriesRequest) Reset() {
	*x = AppendEntriesRequest{}
	mi := &file_proto_raft_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AppendEntriesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesRequest) ProtoMessage() {}

func (x *AppendEntriesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*AppendEntriesRequest) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{3}
}

func (x *AppendEntriesRequest) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntriesRequest) GetLeaderId() string {
	if x != nil {
		return x.LeaderId
	}
	return ""
}

func (x *AppendEntriesRequest) GetPrevLogIndex() uint64 {
	if x != nil {
		return x.PrevLogIndex
	}
	return 0
}

func (x *AppendEntriesRequest) GetPrevLogTerm() uint64 {
	if x != nil {
		return x.PrevLogTerm
	}
	return 0
}

func (x *AppendEntriesRequest) GetEntries() []*LogEntry {
	if x != nil {
		return x.Entries
	}
	return nil
}

func (x *AppendEntriesRequest) GetLeaderCommit() uint64 {
	if x != nil {
		return x.LeaderCommit
	}
	return 0
}

type AppendEntriesResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	Term uint64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`

	Success bool `protobuf:"varint,2,opt,name=success,proto3" json:"success,omitempty"`

	MatchIndex    uint64 `protobuf:"varint,3,opt,name=match_index,json=matchIndex,proto3" json:"match_index,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *AppendEntriesResponse) Reset() {
	*x = AppendEntriesResponse{}
	mi := &file_proto_raft_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AppendEntriesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesResponse) ProtoMessage() {}

func (x *AppendEntriesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*AppendEntriesResponse) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{4}
}

func (x *AppendEntriesResponse) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *AppendEntriesResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *AppendEntriesResponse) GetMatchIndex() uint64 {
	if x != nil {
		return x.MatchIndex
	}
	return 0
}

type InstallSnapshotRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	Term uint64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`

	LeaderId string `protobuf:"bytes,2,opt,name=leader_id,json=leaderId,proto3" json:"leader_id,omitempty"`

	LastIncludedIndex uint64 `protobuf:"varint,3,opt,name=last_included_index,json=lastIncludedIndex,proto3" json:"last_included_index,omitempty"`

	LastIncludedTerm uint64 `protobuf:"varint,4,opt,name=last_included_term,json=lastIncludedTerm,proto3" json:"last_included_term,omitempty"`

	Data          []byte `protobuf:"bytes,5,opt,name=data,proto3" json:"data,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InstallSnapshotRequest) Reset() {
	*x = InstallSnapshotRequest{}
	mi := &file_proto_raft_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InstallSnapshotRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InstallSnapshotRequest) ProtoMessage() {}

func (x *InstallSnapshotRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*InstallSnapshotRequest) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{5}
}

func (x *InstallSnapshotRequest) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *InstallSnapshotRequest) GetLeaderId() string {
	if x != nil {
		return x.LeaderId
	}
	return ""
}

func (x *InstallSnapshotRequest) GetLastIncludedIndex() uint64 {
	if x != nil {
		return x.LastIncludedIndex
	}
	return 0
}

func (x *InstallSnapshotRequest) GetLastIncludedTerm() uint64 {
	if x != nil {
		return x.LastIncludedTerm
	}
	return 0
}

func (x *InstallSnapshotRequest) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

type InstallSnapshotResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	Term          uint64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *InstallSnapshotResponse) Reset() {
	*x = InstallSnapshotResponse{}
	mi := &file_proto_raft_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *InstallSnapshotResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InstallSnapshotResponse) ProtoMessage() {}

func (x *InstallSnapshotResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*InstallSnapshotResponse) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{6}
}

func (x *InstallSnapshotResponse) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

type PutRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Key           []byte                 `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value         []byte                 `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PutRequest) Reset() {
	*x = PutRequest{}
	mi := &file_proto_raft_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PutRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutRequest) ProtoMessage() {}

func (x *PutRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PutRequest) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{7}
}

func (x *PutRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *PutRequest) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type PutResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	Success bool `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`

	Error string `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`

	LeaderHint    string `protobuf:"bytes,3,opt,name=leader_hint,json=leaderHint,proto3" json:"leader_hint,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PutResponse) Reset() {
	*x = PutResponse{}
	mi := &file_proto_raft_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PutResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutResponse) ProtoMessage() {}

func (x *PutResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PutResponse) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{8}
}

func (x *PutResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *PutResponse) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *PutResponse) GetLeaderHint() string {
	if x != nil {
		return x.LeaderHint
	}
	return ""
}

type GetRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Key           []byte                 `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetRequest) Reset() {
	*x = GetRequest{}
	mi := &file_proto_raft_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRequest) ProtoMessage() {}

func (x *GetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetRequest) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{9}
}

func (x *GetRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

type GetResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Value         []byte                 `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	Found         bool                   `protobuf:"varint,2,opt,name=found,proto3" json:"found,omitempty"`
	Error         string                 `protobuf:"bytes,3,opt,name=error,proto3" json:"error,omitempty"`
	LeaderHint    string                 `protobuf:"bytes,4,opt,name=leader_hint,json=leaderHint,proto3" json:"leader_hint,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetResponse) Reset() {
	*x = GetResponse{}
	mi := &file_proto_raft_proto_msgTypes[10]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetResponse) ProtoMessage() {}

func (x *GetResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[10]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*GetResponse) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{10}
}

func (x *GetResponse) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *GetResponse) GetFound() bool {
	if x != nil {
		return x.Found
	}
	return false
}

func (x *GetResponse) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *GetResponse) GetLeaderHint() string {
	if x != nil {
		return x.LeaderHint
	}
	return ""
}

type DeleteRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Key           []byte                 `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeleteRequest) Reset() {
	*x = DeleteRequest{}
	mi := &file_proto_raft_proto_msgTypes[11]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteRequest) ProtoMessage() {}

func (x *DeleteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[11]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*DeleteRequest) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{11}
}

func (x *DeleteRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

type DeleteResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Success       bool                   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Error         string                 `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
	LeaderHint    string                 `protobuf:"bytes,3,opt,name=leader_hint,json=leaderHint,proto3" json:"leader_hint,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DeleteResponse) Reset() {
	*x = DeleteResponse{}
	mi := &file_proto_raft_proto_msgTypes[12]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteResponse) ProtoMessage() {}

func (x *DeleteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[12]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*DeleteResponse) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{12}
}

func (x *DeleteResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *DeleteResponse) GetError() string {
	if x != nil {
		return x.Error
	}
	return ""
}

func (x *DeleteResponse) GetLeaderHint() string {
	if x != nil {
		return x.LeaderHint
	}
	return ""
}

type PingRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	SenderId      string `protobuf:"bytes,1,opt,name=sender_id,json=senderId,proto3" json:"sender_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PingRequest) Reset() {
	*x = PingRequest{}
	mi := &file_proto_raft_proto_msgTypes[13]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PingRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingRequest) ProtoMessage() {}

func (x *PingRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[13]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PingRequest) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{13}
}

func (x *PingRequest) GetSenderId() string {
	if x != nil {
		return x.SenderId
	}
	return ""
}

type PingResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`

	ResponderId string `protobuf:"bytes,1,opt,name=responder_id,json=responderId,proto3" json:"responder_id,omitempty"`

	Message       string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *PingResponse) Reset() {
	*x = PingResponse{}
	mi := &file_proto_raft_proto_msgTypes[14]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PingResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PingResponse) ProtoMessage() {}

func (x *PingResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_raft_proto_msgTypes[14]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*PingResponse) Descriptor() ([]byte, []int) {
	return file_proto_raft_proto_rawDescGZIP(), []int{14}
}

func (x *PingResponse) GetResponderId() string {
	if x != nil {
		return x.ResponderId
	}
	return ""
}

func (x *PingResponse) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

var File_proto_raft_proto protoreflect.FileDescriptor

const file_proto_raft_proto_rawDesc = "" +
	"\n" +
	"\x10proto/raft.proto\x12\x04raft\"H\n" +
	"\bLogEntry\x12\x14\n" +
	"\x05index\x18\x01 \x01(\x04R\x05index\x12\x12\n" +
	"\x04term\x18\x02 \x01(\x04R\x04term\x12\x12\n" +
	"\x04data\x18\x03 \x01(\fR\x04data\"\x95\x01\n" +
	"\x12RequestVoteRequest\x12\x12\n" +
	"\x04term\x18\x01 \x01(\x04R\x04term\x12!\n" +
	"\fcandidate_id\x18\x02 \x01(\tR\vcandidateId\x12$\n" +
	"\x0elast_log_index\x18\x03 \x01(\x04R\flastLogIndex\x12\"\n" +
	"\rlast_log_term\x18\x04 \x01(\x04R\vlastLogTerm\"L\n" +
	"\x13RequestVoteResponse\x12\x12\n" +
	"\x04term\x18\x01 \x01(\x04R\x04term\x12!\n" +
	"\fvote_granted\x18\x02 \x01(\bR\vvoteGranted\"\xe0\x01\n" +
	"\x14AppendEntriesRequest\x12\x12\n" +
	"\x04term\x18\x01 \x01(\x04R\x04term\x12\x1b\n" +
	"\tleader_id\x18\x02 \x01(\tR\bleaderId\x12$\n" +
	"\x0eprev_log_index\x18\x03 \x01(\x04R\fprevLogIndex\x12\"\n" +
	"\rprev_log_term\x18\x04 \x01(\x04R\vprevLogTerm\x12(\n" +
	"\aentries\x18\x05 \x03(\v2\x0e.raft.LogEntryR\aentries\x12#\n" +
	"\rleader_commit\x18\x06 \x01(\x04R\fleaderCommit\"f\n" +
	"\x15AppendEntriesResponse\x12\x12\n" +
	"\x04term\x18\x01 \x01(\x04R\x04term\x12\x18\n" +
	"\asuccess\x18\x02 \x01(\bR\asuccess\x12\x1f\n" +
	"\vmatch_index\x18\x03 \x01(\x04R\n" +
	"matchIndex\"\xbb\x01\n" +
	"\x16InstallSnapshotRequest\x12\x12\n" +
	"\x04term\x18\x01 \x01(\x04R\x04term\x12\x1b\n" +
	"\tleader_id\x18\x02 \x01(\tR\bleaderId\x12.\n" +
	"\x13last_included_index\x18\x03 \x01(\x04R\x11lastIncludedIndex\x12,\n" +
	"\x12last_included_term\x18\x04 \x01(\x04R\x10lastIncludedTerm\x12\x12\n" +
	"\x04data\x18\x05 \x01(\fR\x04data\"-\n" +
	"\x17InstallSnapshotResponse\x12\x12\n" +
	"\x04term\x18\x01 \x01(\x04R\x04term\"4\n" +
	"\n" +
	"PutRequest\x12\x10\n" +
	"\x03key\x18\x01 \x01(\fR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\fR\x05value\"^\n" +
	"\vPutResponse\x12\x18\n" +
	"\asuccess\x18\x01 \x01(\bR\asuccess\x12\x14\n" +
	"\x05error\x18\x02 \x01(\tR\x05error\x12\x1f\n" +
	"\vleader_hint\x18\x03 \x01(\tR\n" +
	"leaderHint\"\x1e\n" +
	"\n" +
	"GetRequest\x12\x10\n" +
	"\x03key\x18\x01 \x01(\fR\x03key\"p\n" +
	"\vGetResponse\x12\x14\n" +
	"\x05value\x18\x01 \x01(\fR\x05value\x12\x14\n" +
	"\x05found\x18\x02 \x01(\bR\x05found\x12\x14\n" +
	"\x05error\x18\x03 \x01(\tR\x05error\x12\x1f\n" +
	"\vleader_hint\x18\x04 \x01(\tR\n" +
	"leaderHint\"!\n" +
	"\rDeleteRequest\x12\x10\n" +
	"\x03key\x18\x01 \x01(\fR\x03key\"a\n" +
	"\x0eDeleteResponse\x12\x18\n" +
	"\asuccess\x18\x01 \x01(\bR\asuccess\x12\x14\n" +
	"\x05error\x18\x02 \x01(\tR\x05error\x12\x1f\n" +
	"\vleader_hint\x18\x03 \x01(\tR\n" +
	"leaderHint\"*\n" +
	"\vPingRequest\x12\x1b\n" +
	"\tsender_id\x18\x01 \x01(\tR\bsenderId\"K\n" +
	"\fPingResponse\x12!\n" +
	"\fresponder_id\x18\x01 \x01(\tR\vresponderId\x12\x18\n" +
	"\amessage\x18\x02 \x01(\tR\amessage2\xa7\x03\n" +
	"\vRaftService\x12B\n" +
	"\vRequestVote\x12\x18.raft.RequestVoteRequest\x1a\x19.raft.RequestVoteResponse\x12H\n" +
	"\rAppendEntries\x12\x1a.raft.AppendEntriesRequest\x1a\x1b.raft.AppendEntriesResponse\x12N\n" +
	"\x0fInstallSnapshot\x12\x1c.raft.InstallSnapshotRequest\x1a\x1d.raft.InstallSnapshotResponse\x12*\n" +
	"\x03Put\x12\x10.raft.PutRequest\x1a\x11.raft.PutResponse\x12*\n" +
	"\x03Get\x12\x10.raft.GetRequest\x1a\x11.raft.GetResponse\x123\n" +
	"\x06Delete\x12\x13.raft.DeleteRequest\x1a\x14.raft.DeleteResponse\x12-\n" +
	"\x04Ping\x12\x11.raft.PingRequest\x1a\x12.raft.PingResponseB\x15Z\x13granite/proto;protob\x06proto3"

var (
	file_proto_raft_proto_rawDescOnce sync.Once
	file_proto_raft_proto_rawDescData []byte
)

func file_proto_raft_proto_rawDescGZIP() []byte {
	file_proto_raft_proto_rawDescOnce.Do(func() {
		file_proto_raft_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_proto_raft_proto_rawDesc), len(file_proto_raft_proto_rawDesc)))
	})
	return file_proto_raft_proto_rawDescData
}

var file_proto_raft_proto_msgTypes = make([]protoimpl.MessageInfo, 15)
var file_proto_raft_proto_goTypes = []any{
	(*LogEntry)(nil),
	(*RequestVoteRequest)(nil),
	(*RequestVoteResponse)(nil),
	(*AppendEntriesRequest)(nil),
	(*AppendEntriesResponse)(nil),
	(*InstallSnapshotRequest)(nil),
	(*InstallSnapshotResponse)(nil),
	(*PutRequest)(nil),
	(*PutResponse)(nil),
	(*GetRequest)(nil),
	(*GetResponse)(nil),
	(*DeleteRequest)(nil),
	(*DeleteResponse)(nil),
	(*PingRequest)(nil),
	(*PingResponse)(nil),
}
var file_proto_raft_proto_depIdxs = []int32{
	0,
	1,
	3,
	5,
	7,
	9,
	11,
	13,
	2,
	4,
	6,
	8,
	10,
	12,
	14,
	8,
	1,
	1,
	1,
	0,
}

func init() { file_proto_raft_proto_init() }
func file_proto_raft_proto_init() {
	if File_proto_raft_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_proto_raft_proto_rawDesc), len(file_proto_raft_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   15,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_raft_proto_goTypes,
		DependencyIndexes: file_proto_raft_proto_depIdxs,
		MessageInfos:      file_proto_raft_proto_msgTypes,
	}.Build()
	File_proto_raft_proto = out.File
	file_proto_raft_proto_goTypes = nil
	file_proto_raft_proto_depIdxs = nil
}
