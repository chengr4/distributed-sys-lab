package main

import (
	"bufio"
	"fmt"
	"io"
	"net/rpc"
	"strconv"
	"strings"
)

// CLI 結構體封裝了輸入、輸出與對外連線狀態
type CLI struct {
	client *rpc.Client
	in     io.Reader
	out    io.Writer
}

// NewCLI 建立一個新的 CLI 實例，支援相依性注入
func NewCLI(in io.Reader, out io.Writer) *CLI {
	return &CLI{
		in:  in,
		out: out,
	}
}

// Run 啟動 CLI 迴圈
func (c *CLI) Run() {
	scanner := bufio.NewScanner(c.in)
	fmt.Fprintln(c.out, "=== Go RPC Distributed System CLI ===")
	fmt.Fprintln(c.out, "可用指令: dial <addr>, add <a> <b>, store <k> <v>, read <k>, getTime, exit")

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
			return

		case "dial":
			if len(args) < 2 {
				fmt.Fprintln(c.out, "用法: dial <位址:連接埠>")
				continue
			}
			client, err := rpc.Dial("tcp", args[1])
			if err != nil {
				fmt.Fprintf(c.out, "連線失敗: %v\n", err)
				continue
			}
			c.client = client
			fmt.Fprintf(c.out, "成功連線至 %s\n", args[1])

		case "getTime":
			if !c.checkConnection() {
				continue
			}
			var reply GetTimeReply
			err := c.client.Call("KVService.GetTime", &GetTimeArgs{}, &reply)
			if err != nil {
				fmt.Fprintf(c.out, "呼叫失敗: %v\n", err)
			} else {
				fmt.Fprintf(c.out, "伺服器時間: %s\n", reply.Time)
			}

		case "add":
			if len(args) < 3 || !c.checkConnection() {
				fmt.Fprintln(c.out, "用法: add <數字1> <數字2>")
				continue
			}
			n1, _ := strconv.Atoi(args[1])
			n2, _ := strconv.Atoi(args[2])
			var reply AddReply
			err := c.client.Call("KVService.Add", &AddArgs{Num1: n1, Num2: n2}, &reply)
			if err != nil {
				fmt.Fprintf(c.out, "呼叫失敗: %v\n", err)
			} else {
				fmt.Fprintf(c.out, "計算結果: %d\n", reply.Result)
			}

		case "store":
			if len(args) < 3 || !c.checkConnection() {
				fmt.Fprintln(c.out, "用法: store <鍵> <值>")
				continue
			}
			var reply StoreReply
			err := c.client.Call("KVService.Store", &StoreArgs{Name: args[1], Value: args[2]}, &reply)
			if err != nil {
				fmt.Fprintf(c.out, "呼叫失敗: %v\n", err)
			} else {
				fmt.Fprintf(c.out, "伺服器回應: %s\n", reply.Message)
			}

		case "read":
			if len(args) < 2 || !c.checkConnection() {
				fmt.Fprintln(c.out, "用法: read <鍵>")
				continue
			}
			var reply ReadReply
			err := c.client.Call("KVService.Read", &ReadArgs{Name: args[1]}, &reply)
			if err != nil {
				fmt.Fprintf(c.out, "呼叫失敗: %v\n", err)
			} else if !reply.Success {
				fmt.Fprintf(c.out, "錯誤: %s\n", reply.Message)
			} else {
				fmt.Fprintf(c.out, "讀取結果: %s (%s)\n", reply.Value, reply.Message)
			}

		default:
			fmt.Fprintln(c.out, "未知指令")
		}
	}
}

func (c *CLI) checkConnection() bool {
	if c.client == nil {
		fmt.Fprintln(c.out, "請先執行 dial 連線至節點")
		return false
	}
	return true
}
