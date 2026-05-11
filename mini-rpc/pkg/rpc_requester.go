package minirpc

import (
	"fmt"
	"net/rpc"
	"time"
)

// It encapsulates rpc.Client and provides the timeout control.
type RPCAdapter struct {
	remoteHandle *rpc.Client
	timeout      time.Duration
}

func NewRPCAdapter(remoteHandle *rpc.Client, timeout time.Duration) *RPCAdapter {
	return &RPCAdapter{
		remoteHandle: remoteHandle,
		timeout:      timeout,
	}
}

func (r *RPCAdapter) CallRemote(serviceMethod string, args any, reply any) error {
	if r.remoteHandle == nil {
		return fmt.Errorf("RPC client is not initialized (not connected)")
	}

	call := r.remoteHandle.Go(serviceMethod, args, reply, nil)
	select {
	case <-call.Done:
		// Successfully Called
		return call.Error
	case <-time.After(r.timeout):
		// Timeout
		return fmt.Errorf("RPC call timed out after %s", r.timeout)
	}
}
