package config

import "time"

type Config struct {
	NodeID string

	ListenAddr string

	Peers []string

	DataDir string

	MemTableMaxBytes int64

	HeartbeatInterval time.Duration

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		MemTableMaxBytes:   64 * 1024 * 1024,
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
	}
}

func (c *Config) Validate() error {
	if c.NodeID == "" {
		return errorf("NodeID must not be empty")
	}
	if c.ListenAddr == "" {
		return errorf("ListenAddr must not be empty")
	}
	if c.DataDir == "" {
		return errorf("DataDir must not be empty")
	}
	if len(c.Peers) == 0 {
		return errorf("Peers must contain at least one peer address")
	}
	if c.HeartbeatInterval <= 0 {
		return errorf("HeartbeatInterval must be positive")
	}
	if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax <= 0 {
		return errorf("ElectionTimeout values must be positive")
	}
	if c.ElectionTimeoutMin >= c.ElectionTimeoutMax {
		return errorf("ElectionTimeoutMin must be less than ElectionTimeoutMax")
	}
	if c.ElectionTimeoutMin < c.HeartbeatInterval*3 {
		return errorf("ElectionTimeoutMin should be at least 3× HeartbeatInterval to avoid false timeouts")
	}
	return nil
}

func (c *Config) ClusterSize() int {
	return 1 + len(c.Peers)
}

func (c *Config) Quorum() int {
	return c.ClusterSize()/2 + 1
}

func errorf(msg string) error {
	return &configError{msg: msg}
}

type configError struct{ msg string }

func (e *configError) Error() string { return "config: " + e.msg }
