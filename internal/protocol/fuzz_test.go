package protocol

import (
	"encoding/json"
	"testing"
)

// FuzzEventEnvelope fuzzes the CloudEvents parser. Whatever arrives, the
// validator must answer yes or no without panicking, and an event it accepts
// must still be decomposable: source parses, type maps to a known feedback
// type, data decodes.
func FuzzEventEnvelope(f *testing.F) {
	seeds := []string{
		`{"specversion":"1.0","id":"a","source":"lymph://app/junction","type":"lymph.feedback.unknown.v1","time":"2026-09-20T00:00:00Z","data":{"feedback_type":"UNKNOWN"}}`,
		`{"specversion":"2.0","id":"a","source":"nope","type":"lymph.feedback.unknown.v1","time":"2026-09-20T00:00:00Z","data":{}}`,
		`{"specversion":"1.0","id":"","source":"lymph:///","type":"","time":"0001-01-01T00:00:00Z","data":null}`,
		`{"specversion":"1.0","id":"a","source":"lymph://app/junction/extra","type":"lymph.feedback.purple.v1","time":"2026-09-20T00:00:00Z","data":{"feedback_type":"PURPLE"}}`,
	}
	for _, seed := range seeds {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		var event Event
		if err := json.Unmarshal(raw, &event); err != nil {
			return
		}
		if err := event.Validate(); err != nil {
			return
		}
		app, junction, err := ParseEventSource(event.Source)
		if err != nil || app == "" || junction == "" {
			t.Fatalf("a validated event has an unusable source %q (%v)", event.Source, err)
		}
		feedback, err := FeedbackTypeOf(event.Type)
		if err != nil || !feedback.Valid() {
			t.Fatalf("a validated event has an unusable type %q (%v)", event.Type, err)
		}
		if _, err := event.DecodeData(); err != nil {
			t.Fatalf("a validated event has undecodable data: %v", err)
		}
	})
}
