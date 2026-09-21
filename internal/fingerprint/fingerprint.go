// Package fingerprint groups noisy raw events into recurring issues
// (section 34 and section 35).
//
// Applications may supply their own fingerprint provider through the junction
// contract. When they do not, the generic fallback below is used:
// application, junction, feedback type, reason code and a normalized payload
// subset.
package fingerprint

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// Prefix marks a fingerprint as a SHA-256 digest, matching the object store's
// hash format.
const Prefix = "sha256:"

// Normalize collapses a free-text payload into a stable pattern, so that
//
//	"Acme business was booked at 512.00"
//	"Beta business was booked at 918.45"
//
// both reduce to
//
//	"<str> business was booked at <num>"
//
// The rules are deliberately narrow and domain-blind (section 35):
//
//   - digit runs, including "512.00", "12,500" and "2026-09-20", become <num>
//   - Titlecase words ("Acme", "Beta", "Tender") become <str>
//   - everything else is lowercased and kept verbatim
//
// Narrow is the point. Masking every word would merge genuinely different
// problems into one issue, which is worse than splitting them: the issue list
// would hide a distinct failure behind a confident-looking count. Keeping
// lowercase and all-caps tokens (units such as MT, states such as NEW, the
// words that carry meaning) means two failures only group when the words that
// carry meaning are the same words.
//
// This never alters the stored raw events; it only derives a grouping key.
// When a junction knows its own domain (section 2: Lymph does not), it should
// supply its own pattern instead of relying on this.
func Normalize(s string) string {
	runes := []rune(s)
	var tokens []string
	var word, num []rune
	titlecase := true

	flushWord := func() {
		if len(word) > 0 {
			if titlecase && len(word) > 1 {
				tokens = append(tokens, "<str>")
			} else {
				tokens = append(tokens, string(word))
			}
			word = word[:0]
		}
	}
	flushNum := func() {
		if len(num) > 0 {
			tokens = append(tokens, "<num>")
			num = num[:0]
		}
	}

	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case unicode.IsDigit(r):
			flushWord()
			num = append(num, r)
		case isNumericSeparator(r) && len(num) > 0 && i+1 < len(runes) && unicode.IsDigit(runes[i+1]):
			// "512.00", "12,500" and "2026-09-20" each stay one number token.
			num = append(num, r)
		case unicode.IsLetter(r):
			flushNum()
			if len(word) == 0 {
				titlecase = unicode.IsUpper(r) || unicode.IsTitle(r)
			} else if unicode.IsUpper(r) || unicode.IsTitle(r) {
				// An embedded capital ("McDermott", "ACME") is not the
				// Titlecase shape, so it stays literal.
				titlecase = false
			}
			word = append(word, unicode.ToLower(r))
		default:
			flushWord()
			flushNum()
		}
	}
	flushWord()
	flushNum()
	return strings.Join(tokens, " ")
}

func isNumericSeparator(r rune) bool {
	return r == '.' || r == ',' || r == '-'
}

// Part is one element of a fingerprint key.
type Part struct {
	Name  string
	Value string
	// Raw marks a value that is used verbatim instead of being normalized.
	Raw bool
}

func (p Part) render() string {
	v := p.Value
	if !p.Raw {
		v = Normalize(v)
	}
	return p.Name + "=" + v
}

// Key hashes ordered fingerprint parts into a stable identifier.
func Key(parts ...Part) string {
	rendered := make([]string, 0, len(parts))
	for _, p := range parts {
		if p.Value == "" {
			continue
		}
		rendered = append(rendered, p.render())
	}
	sum := sha256.Sum256([]byte(strings.Join(rendered, "\n")))
	return Prefix + hex.EncodeToString(sum[:])
}

// Generic is the fallback fingerprint from section 35.
func Generic(applicationID, junctionID, feedbackType, reasonCode, payload string) string {
	return Key(
		Part{Name: "app", Value: applicationID, Raw: true},
		Part{Name: "junction", Value: junctionID, Raw: true},
		Part{Name: "feedback", Value: feedbackType, Raw: true},
		Part{Name: "reason", Value: reasonCode, Raw: true},
		Part{Name: "payload", Value: payload},
	)
}

// Pattern renders the human-readable normalized pattern for display.
func Pattern(payload string) string { return Normalize(payload) }
