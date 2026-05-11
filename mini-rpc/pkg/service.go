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
	nextNodeHandle RemoteRequester
}

func NewKVService(s KVStore) *KVService {
	return &KVService{
		storage: s,
		// nextNode is nil until SetNextNode is called
	}
}

func (s *KVService) Store(args *StoreArgs, reply *StoreReply) error {
	log.Printf("[Server] Received Store request: Key=%s, Value=%s\n", args.Name, args.Value)

	log.Printf("[Server] Executing local storage set operation.\n")
	s.storage.Set(args.Name, args.Value)

	reply.Success = true
	reply.Message = fmt.Sprintf("Store %s successfully", args.Name)
	log.Printf("[Server] Store operation completed. Sending response.\n")

	return nil
}

func (s *KVService) Read(args *ReadArgs, reply *ReadReply) error {
	log.Printf("[Server] Received Read request: Key=%s\n", args.Name)

	log.Printf("[Server] Executing local storage get operation.\n")
	value, ok := s.storage.Get(args.Name)
	if !ok {
		log.Printf("[Server] Read failed: Key %s not found.\n", args.Name)
		reply.Success = false
		reply.Message = fmt.Sprintf("Key %s not found", args.Name)
		return nil
	}

	reply.Success = true
	reply.Value = value
	reply.Message = fmt.Sprintf("Read %s successfully", args.Name)
	log.Printf("[Server] Read operation successful. Sending response.\n")

	return nil
}

func (s *KVService) Add(args *AddArgs, reply *AddReply) error {
	log.Printf("[Server] Received Add request: %d + %d\n", args.Num1, args.Num2)

	// Forwarding
	if s.nextNodeHandle != nil {
		log.Printf("[Server] Next node detected. Forwarding request sent to next node\n")

		return s.nextNodeHandle.CallRemote("KVService.Add", args, reply)

	}

	log.Printf("[Server] No next node configured. Executing local Add calculation.\n")
	reply.Success = true
	reply.Result = args.Num1 + args.Num2
	log.Printf("[Server] Local calculation completed. Sending response.\n")

	return nil
}

func (s *KVService) GetTime(args *GetTimeArgs, reply *GetTimeReply) error {
	log.Printf("[Server] Received GetTime request.\n")

	reply.Success = true
	//RFC3339: 2026-05-08T15:04:05Z
	reply.Time = time.Now().Format(time.RFC3339)
	log.Printf("[Server] Time fetched: %s. Sending response.\n", reply.Time)

	return nil
}

func (s *KVService) SetNextNode(args *SetNextNodeArgs, reply *SetNextNodeReply) error {
	log.Printf("[Server] Received SetNextNode request. Target: %s\n", args.NextNodeAddr)

	log.Printf("[Server] Attempting to establish TCP connection via RPC Dial...\n")
	nextNodeHandle, err := rpc.Dial("tcp", args.NextNodeAddr)
	if err != nil {
		log.Printf("[Server] Connection failed: %v\n", err)
		reply.Success = false
		reply.Message = fmt.Sprintf("Failed to connect to next node: %v", err)
		return nil
	}

	// TO FIX: Inner layer depends on outer layer
	s.nextNodeHandle = NewRPCAdapter(nextNodeHandle, 3*time.Second)
	reply.Success = true
	reply.Message = fmt.Sprintf("Successfully connected to next node: %s", args.NextNodeAddr)
	log.Printf("[Server] Connection established and stored. Sending success confirmation.\n")

	return nil
}
