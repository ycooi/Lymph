package fingerprint

import (
	"strings"
	"testing"
)

func TestNormalizeCollapsesVariation(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Acme business was booked at 512.00", "<str> business was booked at <num>"},
		{"Beta business was booked at 918.45", "<str> business was booked at <num>"},
		{"RCF Tender 2026-09-20: 12,500 MT", "rcf <str> <num> <num> mt"},
		{"acme trader said yes", "acme trader said yes"},
	}
	for _, tc := range cases {
		if got := Normalize(tc.in); got != tc.want {
			t.Errorf("Normalize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestNormalizeKeepsMeaning documents the deliberate boundary of the generic
// normalizer: it masks the parts of a payload that vary between occurrences of
// the same problem (numbers, proper nouns) and keeps the words that carry
// meaning. Over-masking would be worse than under-masking, because it would
// hide a distinct failure behind a confident-looking count.
func TestNormalizeKeepsMeaning(t *testing.T) {
	a := Normalize(`{"text":"Acme business was booked at 512.00","src":"email-1"}`)
	b := Normalize(`{"text":"Beta business was booked at 918.45","src":"email-93"}`)
	if a != b {
		t.Fatalf("occurrences of the same problem must normalize alike:\n  %q\n  %q", a, b)
	}
	c := Normalize(`{"text":"Acme is not a buyer"}`)
	if a == c {
		t.Fatal("different problems must not collapse into one issue")
	}
	if !strings.Contains(a, "<num>") || !strings.Contains(a, "<str>") {
		t.Fatalf("expected both <num> and <str> masks, got %q", a)
	}
}

// TestGenericGroupsEquivalentPayloads covers the fallback from section 35.
func TestGenericGroupsEquivalentPayloads(t *testing.T) {
	base := `{"text":"Acme business was booked at 512.00","src":"email-1"}`
	sameShape := `{"text":"Acme business was booked at 918.45","src":"email-9"}`

	a := Generic("app-1", "junc-1", "UNKNOWN", "UNKNOWN_EVENT_PHRASE", base)
	b := Generic("app-1", "junc-1", "UNKNOWN", "UNKNOWN_EVENT_PHRASE", sameShape)
	if a != b {
		t.Errorf("payloads differing only in numbers produced different fingerprints:\n  %s\n  %s", a, b)
	}
	if c := Generic("app-1", "junc-1", "UNKNOWN", "OTHER_REASON", base); a == c {
		t.Error("different reason codes must not share a fingerprint")
	}
	if d := Generic("app-2", "junc-1", "UNKNOWN", "UNKNOWN_EVENT_PHRASE", base); a == d {
		t.Error("different applications must not share a fingerprint")
	}
	if e := Generic("app-1", "junc-2", "UNKNOWN", "UNKNOWN_EVENT_PHRASE", base); a == e {
		t.Error("different junctions must not share a fingerprint")
	}
}

// TestJunctionSuppliedFingerprintIsRaw shows how an application collapses the
// variation the generic normalizer deliberately leaves alone. The junction
// knows what AWARDED means and what an actor is; Lymph does not (section 2,
// section 100), so the junction hands over a pattern and Lymph hashes it
// verbatim.
func TestJunctionSuppliedFingerprintIsRaw(t *testing.T) {
	pattern := "<ACTOR> business was booked at <PRICE>"
	fingerprintFor := func() string {
		return Key(
			Part{Name: "app", Value: "app-1", Raw: true},
			Part{Name: "junction", Value: "junc-1", Raw: true},
			Part{Name: "template", Value: pattern, Raw: true},
		)
	}
	first, second := fingerprintFor(), fingerprintFor()
	if first != second {
		t.Fatal("a junction-supplied pattern must hash to one stable fingerprint")
	}
	if !strings.HasPrefix(first, Prefix) {
		t.Fatalf("fingerprints are content addresses, got %q", first)
	}
}
