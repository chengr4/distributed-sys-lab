package main

import (
	"flag"
	"log"
	"os"
	"mini-rpc/pkg"
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
	cli := minirpc.NewCLI(os.Stdin, os.Stdout)
	shouldExit := cli.Run()

	if shouldExit {
		os.Exit(0)
	}

	select {}
}
