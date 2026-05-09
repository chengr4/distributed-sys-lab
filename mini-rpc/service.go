package main

import (
	"fmt"
	"log"
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

type KVService struct {
	storage *Storage
}

func NewKVService(s *Storage) *KVService {
	return &KVService{
		storage: s,
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
