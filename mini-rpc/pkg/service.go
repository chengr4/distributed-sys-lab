package minirpc

import (
	"fmt"
	"log"
	"net/rpc"
	"time"
)

type AddArgs struct {
	Num1 int
	Num2 int
}

type AddReply struct {
	Success bool
	Result  int
}

type SetNextNodeArgs struct {
	NextNodeAddr string
}

type SetNextNodeReply struct {
	Success bool
	Message string
}

type GetTimeArgs struct{}

type GetTimeReply struct {
	Success bool
	Time    string
}

type StoreArgs struct {
	Name  string
	Value string
}

type StoreReply struct {
	Success bool
	Message string
}

type ReadArgs struct {
	Name string
}

type ReadReply struct {
	Success bool
	Value   string
	Message string
}

type KVStore interface {
	Set(key string, value string)
	Get(key string) (string, bool)
}

type KVService struct {
	storage        KVStore
	nextNodeHandle *rpc.Client
}

func NewKVService(s KVStore) *KVService {
	return &KVService{
		storage: s,
		// nextNode is nil until SetNextNode is called
	}
}

func (s *KVService) Store(args *StoreArgs, reply *StoreReply) error {
	log.Printf("[Server] Got Store request: %s: %s\n", args.Name, args.Value)

	s.storage.Set(args.Name, args.Value)

	reply.Success = true
	reply.Message = fmt.Sprintf("Store %s successfully", args.Name)

	return nil
}

func (s *KVService) Read(args *ReadArgs, reply *ReadReply) error {
	log.Printf("[Server] Got Read request: %s\n", args.Name)

	value, ok := s.storage.Get(args.Name)
	if !ok {
		reply.Success = false
		reply.Message = fmt.Sprintf("Key %s not found", args.Name)
		return nil
	}

	reply.Success = true
	reply.Value = value
	reply.Message = fmt.Sprintf("Read %s successfully", args.Name)

	return nil
}

func (s *KVService) Add(args *AddArgs, reply *AddReply) error {
	log.Printf("[Server] Got Add request: %d + %d\n", args.Num1, args.Num2)

	// Forwarding
	if s.nextNodeHandle != nil {
		log.Printf("[Server] Forwarding Add request to next node\n")
		return s.nextNodeHandle.Call("KVService.Add", args, reply)
	}

	reply.Success = true
	reply.Result = args.Num1 + args.Num2

	return nil
}

func (s *KVService) GetTime(args *GetTimeArgs, reply *GetTimeReply) error {
	log.Printf("[Server] Got GetTime request\n")

	reply.Success = true
	//RFC3339: 2026-05-08T15:04:05Z
	reply.Time = time.Now().Format(time.RFC3339)

	return nil
}

func (s *KVService) SetNextNode(args *SetNextNodeArgs, reply *SetNextNodeReply) error {
	log.Printf("[Server] Setting up the connection to the next node: %s", args.NextNodeAddr)

	nextNodeHandle, err := rpc.Dial("tcp", args.NextNodeAddr)
	if err != nil {
		reply.Success = false
		reply.Message = fmt.Sprintf("Failed to connect to next node: %v", err)
		return nil
	}

	s.nextNodeHandle = nextNodeHandle
	reply.Success = true
	reply.Message = fmt.Sprintf("Successfully connected to next node: %s", args.NextNodeAddr)

	return nil
}
