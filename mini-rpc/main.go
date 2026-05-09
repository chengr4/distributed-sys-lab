package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
)

func startServer(port string) error {
	storage := NewStorage()
	service := NewKVService(storage)

	// Register all methods of KVService that comply with rpc rules
	err := rpc.Register(service)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept failed: %v", err)
			continue
		}

		// keep reading rpc requests from the connection until it is closed
		go rpc.ServeConn(conn)
	}
}

// Only focus on reading parameters; startServer manages the startup and listening.
func main() {
	// (-port, default, -help)
	port := flag.String("port", "1234", "The port to listen on")
	flag.Parse()

	log.Printf("[Node] The server starts, listening to Port %s...\n", *port)

	if err := startServer(*port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
