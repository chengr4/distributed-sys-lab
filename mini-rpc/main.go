package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
)

func startServer(port string) error {
	storage := NewStorage()
	service := NewKVService(storage)

	// Create a private RPC server instance instead of using the global one
	server := rpc.NewServer()
	err := server.Register(service)
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

		// Use the private server instance to handle connections
		go server.ServeConn(conn)
	}
}

// Only focus on reading parameters; startServer manages the startup and listening.
func main() {
	// (-port, default, -help)
	port := flag.String("port", "1234", "The port to listen on")
	flag.Parse()

	log.Printf("[Node] The server starts, listening to Port %s...\n", *port)

	// Start Server in the background
	go func() {
		if err := startServer(*port); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Start CLI in the foreground
	cli := NewCLI(os.Stdin, os.Stdout)
	shouldExit := cli.Run()

	// If the user explicitly typed 'exit', terminate the process.
	// Otherwise (e.g., EOF on background node), keep the server alive.
	if shouldExit {
		os.Exit(0)
	}

	select {}
}
