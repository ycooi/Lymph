package fingerprint

import (
	"strings"
	"testing"
	"unicode"
)

// FuzzNormalize checks the properties grouping depends on: no panic, a
// deterministic result, and an output with no digits and no capitals left in it
// — the two things that let thousands of occurrences share one fingerprint.
//
// Idempotence is deliberately *not* asserted: Normalize is applied to raw
// payloads, never to its own output, and its placeholders ("<num>") are not
// themselves payload text.
func FuzzNormalize(f *testing.F) {
	for _, seed := range []string{
		"", "Acme business was booked at 512.00", "RCF Tender 2026-09-20: 12,500 MT",
		`{"text":"awarded 50,000 mt"}`, "\x00\xff\xfe", "１２３４", "ᚠᚢᚦ",
		// Found by this fuzzer: a letter with no lowercase mapping.
		"ϒ 12",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		once := Normalize(input)
		if twice := Normalize(input); twice != once {
			t.Fatalf("normalisation is not deterministic:\n  %q\n  %q", once, twice)
		}
		for _, r := range once {
			if unicode.IsDigit(r) {
				t.Fatalf("a digit survived normalisation: %q", once)
			}
			// ASCII capitals always have a lowercase form, so none may survive.
			// A handful of Unicode letters (U+03D2, for instance) have no
			// lowercase mapping at all; those are left as they are, which is
			// deterministic and therefore still groupable.
			if r >= 'A' && r <= 'Z' {
				t.Fatalf("an ASCII capital survived normalisation: %q", once)
			}
		}
		if strings.Contains(once, "  ") {
			t.Fatalf("normalisation left a double space: %q", once)
		}
		if key := Key(Part{Name: "payload", Value: input}); len(key) != len(Prefix)+64 {
			t.Fatalf("fingerprint %q is not a content address", key)
		}
	})
}
