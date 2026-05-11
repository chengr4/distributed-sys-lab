package minirpc

import "testing"

type MockDialer struct {
	CalledWithAddr string
}

func TestKVServiceStore(t *testing.T) {
	storage := NewStorage()
	service := NewKVService(storage, &MockDialer{})

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
	service := NewKVService(storage, &MockDialer{})

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
	service := NewKVService(nil, &MockDialer{})

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
	service := NewKVService(nil, &MockDialer{})

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

func (m *MockDialer) Dial(addr string) (RemoteRequester, error) {
	m.CalledWithAddr = addr

	return &MockTimeoutRequester{}, nil
}

func TestService_SetNextNode_NoNetwork(t *testing.T) {
	mockDialer := &MockDialer{}

	service := NewKVService(nil, mockDialer)

	args := &SetNextNodeArgs{NextNodeAddr: "localhost:9999"}
	reply := &SetNextNodeReply{}
	err := service.SetNextNode(args, reply)
	if err != nil || !reply.Success {
		t.Errorf("Expected success, got error: %v, reply: %+v", err, reply)
	}

	if mockDialer.CalledWithAddr != "localhost:9999" {
		t.Errorf("Expected dialer to be called with 'localhost:9999', got: %s", mockDialer.CalledWithAddr)
	}
}
