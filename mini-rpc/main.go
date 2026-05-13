package main

import (
	"flag"
	"log"
	"mini-rpc/pkg"
	"os"
	"time"
)

// main is now just a thin entry point that calls the minirpc library.
func main() {
	port := flag.String("port", "1234", "The port to listen on")
	flag.Parse()

	log.Printf("[Node] The server starts, listening to Port %s...\n", *port)

	// Start Server in the background using the library
	go func() {
		if err := minirpc.StartServer(*port); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Start CLI in the foreground using the library
	client_dialer := &minirpc.RPCDialer{
		ConnectTimeout: 3 * time.Second,
		RequestTimeout: 5 * time.Second,
	}
	cli := minirpc.NewCLI(os.Stdin, os.Stdout, client_dialer)
	shouldExit := cli.Run()

	if shouldExit {
		os.Exit(0)
	}

	select {}
}
