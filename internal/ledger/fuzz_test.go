package ledger

import "testing"

// FuzzLedgerRecordLine feeds arbitrary bytes to the record decoder. The decoder
// must never panic, and any record it accepts must survive a hash recomputation
// (the same operation verification performs on every startup).
func FuzzLedgerRecordLine(f *testing.F) {
	seeds := []string{
		`{"sequence":1,"kind":"EVENT","object_id":"x","timestamp":"2026-09-20T00:00:00Z","payload":{"a":1},"previous_record_hash":"","record_hash":"sha256:00"}`,
		`{}`,
		`{"sequence":0,"kind":"","object_id":"","timestamp":"","payload":null,"previous_record_hash":"","record_hash":""}`,
		`{"sequence":18446744073709551615,"kind":"EVENT","payload":[1,2,3]}`,
		`{"sequence":1,"kind":"EVENT","payload":{"deep":{"deeper":{"deepest":[1,{"a":null}]}}}}`,
		`null`,
		`[]`,
	}
	for _, seed := range seeds {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		rec, err := decodeRecordLine(raw)
		if err != nil {
			return
		}
		hash := Hash(rec.PrevHash, rec.Sequence, rec.Kind, rec.ObjectID, rec.Timestamp, rec.Payload)
		if len(hash) != len("sha256:")+64 {
			t.Fatalf("hash %q is not a content address", hash)
		}
		if again := Hash(rec.PrevHash, rec.Sequence, rec.Kind, rec.ObjectID, rec.Timestamp, rec.Payload); again != hash {
			t.Fatalf("hash is not deterministic: %q vs %q", hash, again)
		}
	})
}
