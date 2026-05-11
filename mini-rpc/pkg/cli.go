package minirpc

import (
	"bufio"
	"fmt"
	"io"
	"net/rpc"
	"strconv"
	"strings"
	"time"
)

// CLI struct encapsulates input, output, and remote connection state
type CLI struct {
	in              io.Reader
	out             io.Writer
	remoteRequester RemoteRequester
}

// NewCLI creates a new CLI instance with dependency injection support
func NewCLI(in io.Reader, out io.Writer) *CLI {
	return &CLI{
		in:  in,
		out: out,
	}
}

// Run starts the CLI main loop. Returns true if the user explicitly exits,
// false if the input stream ends (EOF).
func (c *CLI) Run() bool {
	scanner := bufio.NewScanner(c.in)
	fmt.Fprintln(c.out, "=== Go RPC Distributed System CLI ===")
	fmt.Fprintln(c.out, "Available commands: dial <addr>, setNextNode <addr>, add <a> <b>, store <k> <v>, read <k>, getTime, exit")

	for {
		fmt.Fprint(c.out, "> ")
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		args := strings.Fields(line)
		if len(args) == 0 {
			continue
		}

		switch args[0] {
		case "exit":
			fmt.Fprintln(c.out, "Goodbye!")
			return true

		case "dial":
			if len(args) < 2 {
				fmt.Fprintln(c.out, "Usage: dial <address:port>")
				continue
			}
			handle, err := rpc.Dial("tcp", args[1])
			if err != nil {
				fmt.Fprintf(c.out, "Dial failed: %v\n", err)
				continue
			}
			c.remoteRequester = NewRPCAdapter(handle, 5*time.Second)
			fmt.Fprintf(c.out, "Successfully connected to %s\n", args[1])

		case "setNextNode":
			if len(args) < 2 {
				fmt.Fprintln(c.out, "Usage: setNextNode <address:port>")
				continue
			}
			if !c.checkConnection() {
				continue
			}
			var reply SetNextNodeReply
			err := c.remoteRequester.CallRemote("KVService.SetNextNode", &SetNextNodeArgs{NextNodeAddr: args[1]}, &reply)
			if err != nil {
				fmt.Fprintf(c.out, "Call failed: %v\n", err)
			} else {
				fmt.Fprintf(c.out, "Server response: %s\n", reply.Message)
			}

		case "getTime":
			if !c.checkConnection() {
				continue
			}
			var reply GetTimeReply
			err := c.remoteRequester.CallRemote("KVService.GetTime", &GetTimeArgs{}, &reply)
			if err != nil {
				fmt.Fprintf(c.out, "Call failed: %v\n", err)
			} else {
				fmt.Fprintf(c.out, "Server time: %s\n", reply.Time)
			}

		case "add":
			if len(args) < 3 {
				fmt.Fprintln(c.out, "Usage: add <num1> <num2>")
				continue
			}
			n1, err1 := strconv.Atoi(args[1])
			n2, err2 := strconv.Atoi(args[2])
			if err1 != nil || err2 != nil {
				fmt.Fprintln(c.out, "Error: Both arguments must be integers")
				continue
			}
			if !c.checkConnection() {
				continue
			}

			var reply AddReply
			err := c.remoteRequester.CallRemote("KVService.Add", &AddArgs{Num1: n1, Num2: n2}, &reply)
			if err != nil {
				fmt.Fprintf(c.out, "Call failed: %v\n", err)
			} else {
				fmt.Fprintf(c.out, "Calculation result: %d\n", reply.Result)
			}

		case "store":
			if len(args) < 3 {
				fmt.Fprintln(c.out, "Usage: store <key> <value>")
				continue
			}
			if !c.checkConnection() {
				continue
			}
			var reply StoreReply
			err := c.remoteRequester.CallRemote("KVService.Store", &StoreArgs{Name: args[1], Value: args[2]}, &reply)
			if err != nil {
				fmt.Fprintf(c.out, "Call failed: %v\n", err)
			} else {
				fmt.Fprintf(c.out, "Server response: %s\n", reply.Message)
			}

		case "read":
			if len(args) < 2 {
				fmt.Fprintln(c.out, "Usage: read <key>")
				continue
			}
			if !c.checkConnection() {
				continue
			}
			var reply ReadReply
			err := c.remoteRequester.CallRemote("KVService.Read", &ReadArgs{Name: args[1]}, &reply)
			if err != nil {
				fmt.Fprintf(c.out, "Call failed: %v\n", err)
			} else if !reply.Success {
				fmt.Fprintf(c.out, "Error: %s\n", reply.Message)
			} else {
				fmt.Fprintf(c.out, "Read result: %s (%s)\n", reply.Value, reply.Message)
			}

		default:
			fmt.Fprintln(c.out, "Unknown command")
		}
	}
	return false
}

func (c *CLI) checkConnection() bool {
	if c.remoteRequester == nil {
		fmt.Fprintln(c.out, "Please execute 'dial' to connect to a node first")
		return false
	}
	return true
}
