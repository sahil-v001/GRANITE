package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"

	"granite/internal/storage"
)

type StateMachine struct {
	mu            sync.Mutex
	storageEngine *storage.Engine
	applyCh       chan ApplyMsg
	lastApplied   uint64
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewStateMachine(engine *storage.Engine, applyCh chan ApplyMsg) *StateMachine {
	ctx, cancel := context.WithCancel(context.Background())
	sm := &StateMachine{
		storageEngine: engine,
		applyCh:       applyCh,
		lastApplied:   0,
		ctx:           ctx,
		cancel:        cancel,
	}
	return sm
}

func (sm *StateMachine) Start() {
	go sm.runApplier()
}

func (sm *StateMachine) Stop() {
	sm.cancel()
}

func (sm *StateMachine) LastApplied() uint64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.lastApplied
}

func (sm *StateMachine) runApplier() {
	for {
		select {
		case <-sm.ctx.Done():
			return
		case msg, ok := <-sm.applyCh:
			if !ok {
				return
			}
			sm.handleApplyMsg(msg)
		}
	}
}

func (sm *StateMachine) handleApplyMsg(msg ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if msg.CommandValid {
		if msg.CommandIndex <= sm.lastApplied {
			return
		}

		if err := sm.applyCommand(msg.Command); err != nil {
			slog.Error("state_machine: failed to apply command", "index", msg.CommandIndex, "error", err)
		} else {
			sm.lastApplied = msg.CommandIndex
			slog.Debug("state_machine: command applied successfully", "index", msg.CommandIndex)
		}
	} else if msg.SnapshotValid {
		if msg.SnapshotIndex <= sm.lastApplied {
			return
		}
		sm.lastApplied = msg.SnapshotIndex
		slog.Info("state_machine: snapshot applied", "index", msg.SnapshotIndex)
	}
}

func EncodeCommand(op storage.OpType, key, value []byte) []byte {
	buf := make([]byte, 1+2+4+len(key)+len(value))
	buf[0] = byte(op)
	binary.LittleEndian.PutUint16(buf[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[3:7], uint32(len(value)))
	copy(buf[7:7+len(key)], key)
	copy(buf[7+len(key):], value)
	return buf
}

func (sm *StateMachine) applyCommand(data []byte) error {
	if len(data) < 7 {
		return fmt.Errorf("command data too short: %d bytes", len(data))
	}

	op := storage.OpType(data[0])
	keyLen := int(binary.LittleEndian.Uint16(data[1:3]))
	valueLen := int(binary.LittleEndian.Uint32(data[3:7]))

	if len(data) < 7+keyLen+valueLen {
		return fmt.Errorf("command data truncated")
	}

	key := data[7 : 7+keyLen]
	value := data[7+keyLen : 7+keyLen+valueLen]

	switch op {
	case storage.OpPut:
		return sm.storageEngine.Put(key, value)
	case storage.OpDelete:
		return sm.storageEngine.Delete(key)
	default:
		return fmt.Errorf("unknown OpType: %d", op)
	}
}
