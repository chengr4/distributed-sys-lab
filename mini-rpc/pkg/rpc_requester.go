package minirpc

import (
	"fmt"
	"log"
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

	log.Printf("[Client] Message prepared for %s\n", serviceMethod)
	log.Printf("[Client] Sending request to %s...\n", serviceMethod)

	call := r.remoteHandle.Go(serviceMethod, args, reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			log.Printf("[Client] Response received from %s with error: %v\n", serviceMethod, call.Error)
			return call.Error
		}
		log.Printf("[Client] Response received from %s successfully\n", serviceMethod)
		return nil
	case <-time.After(r.timeout):
		log.Printf("[Client] RPC call to %s timed out after %s\n", serviceMethod, r.timeout)
		return fmt.Errorf("RPC call timed out after %s", r.timeout)
	}
}

type RPCDialer struct {
	DefaultTimeout time.Duration
}

func (d *RPCDialer) Dial(addr string) (RemoteRequester, error) {
	remoteHandle, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial RPC server at %s: %v", addr, err)
	}

	return NewRPCAdapter(remoteHandle, d.DefaultTimeout), nil
}
