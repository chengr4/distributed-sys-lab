package raft

import (
	"encoding/json"
	"testing"
)

func TestProtocolJSONFormat(t *testing.T) {
	// 理由：確保欄位名稱為 snake_case，這對 Rust (serde) 互通至關重要。
	args := RequestVoteArgs{
		Term:        1,
		CandidateID: "node-1",
	}

	data, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}

	expected := `{"term":1,"candidate_id":"node-1","last_log_index":0,"last_log_term":0}`
	if string(data) != expected {
		t.Errorf("JSON format mismatch!\nExpected: %s\nGot: %s", expected, string(data))
	}
}
