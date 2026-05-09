package main

import "testing"

func TestKVServiceStore(t *testing.T) {
	storage := NewStorage()
	service := NewKVService(storage)

	// net/rpc sets a policy that if you want other node to call your function
	// your should only have one parameter and one return

	// These two are communication contract
	args := &StoreArgs{
		Name:  "kevin",
		Value: "pig",
	}

	reply := &StoreReply{}

	err := service.Store(args, reply)
	if err != nil {
		t.Errorf("%v", err)
	}
	if !reply.Success {
		t.Error("Reply should be labeled as Success!")
	}

	val, ok := storage.Get("kevin")
	if !ok || val != "pig" {
		t.Errorf("Store failed")
	}

}

func TestKVServiceRead(t *testing.T) {
	storage := NewStorage()
	service := NewKVService(storage)

	storage.Set("task", "finish-rpc")

	args := &ReadArgs{Name: "task"}
	reply := &ReadReply{}

	err := service.Read(args, reply)
	if err != nil {
		t.Errorf("%v", err)
	}

	if !reply.Success {
		t.Error("Reply should be labeled as Success!")
	}

	if reply.Value != "finish-rpc" {
		t.Errorf("Read failed, expected: %s, got: %s", "finish-rpc", reply.Value)
	}
}

func TestKVServiceAdd(t *testing.T) {
	service := NewKVService(nil)

	// These two are communication contract
	args := &AddArgs{
		Num1: 15,
		Num2: 27,
	}

	reply := &AddReply{}

	err := service.Add(args, reply)
	if err != nil {
		t.Errorf("%v", err)
	}
	if !reply.Success {
		t.Error("Reply should be labeled as Success!")
	}
	if reply.Result != 42 {
		t.Errorf("Add failed, expected: %d, got: %d", 42, reply.Result)
	}
}

func TestKVServiceGetTime(t *testing.T) {
	service := NewKVService(nil)

	args := &GetTimeArgs{}
	reply := &GetTimeReply{}

	err := service.GetTime(args, reply)
	if err != nil {
		t.Errorf("%v", err)
	}
	if !reply.Success {
		t.Error("Reply should be labeled as Success!")
	}
	if reply.Time == "" {
		t.Error("GetTime failed, expected non-empty time string")
	}

	t.Logf("T_{server}: %s", reply.Time)
}
