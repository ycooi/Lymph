package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Hash computes the chained record hash.
//
// Framing is length-prefixed so that no combination of payload bytes can be
// confused with the surrounding fields, and so verification does not depend on
// JSON marshalling being byte-identical across encoder versions:
//
//	lymph-ledger-v1
//	<sequence>
//	<kind>
//	<object id>
//	<timestamp>
//	<previous record hash>
//	<payload length>
//	<payload bytes>
//
// The version marker lets a future format change invalidate old hashes
// explicitly instead of silently reinterpreting them (section 85).
func Hash(prev string, seq uint64, kind Kind, objectID, ts string, payload []byte) string {
	h := sha256.New()
	header := fmt.Sprintf("lymph-ledger-v1\n%d\n%s\n%s\n%s\n%s\n%d\n", seq, kind, objectID, ts, prev, len(payload))
	h.Write([]byte(header))
	h.Write(payload)
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}
