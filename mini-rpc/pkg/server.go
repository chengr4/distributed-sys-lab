package minirpc

import (
	"log"
	"net"
	"net/rpc"
	"time"
)

// StartServer initializes the storage and service, and starts listening for RPC connections.
func StartServer(port string) error {
	storage := NewStorage()
	server_dialer := &RPCDialer{
		ConnectTimeout: 2 * time.Second,
		RequestTimeout: 3 * time.Second,
	}
	service := NewKVService(storage, server_dialer)

	// Create a private RPC server instance
	server := rpc.NewServer()
	err := server.Register(service)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	// We don't close listener here because it needs to run forever

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept failed: %v", err)
			continue
		}

		// Use the private server instance to handle connections
		go server.ServeConn(conn)
	}
}
