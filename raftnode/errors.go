package raftnode

import "errors"

var (
	ErrStopped       = errors.New("raft node:server stopped")
	ErrLeaderChanged = errors.New("raft node:leader changed")
	ErrTimeout       = errors.New("raft node:request timeout")
)
