package engine

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/objectstore"
)

// FuzzStructuralValidation fuzzes the candidate gate that runs before anything
// becomes a candidate: it must always answer, never panic, and never let a
// bundle through that carries a NUL byte in a text format.
func FuzzStructuralValidation(f *testing.F) {
	for _, seed := range []string{"version: 1\n", "{\"a\":1}", "key: [unclosed", "\x00\x01", ""} {
		f.Add(seed, "config.yaml")
	}
	f.Add("valid: true\n", "notes.txt")
	f.Add("a,b,c", "table.csv")

	f.Fuzz(func(t *testing.T, body, name string) {
		if strings.ContainsRune(name, '/') || name == "" {
			return
		}
		bundle := objectstore.Bundle{name: []byte(body)}
		err := ValidateBundleStructure(bundle)
		lower := strings.ToLower(name)
		isText := strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml") ||
			strings.HasSuffix(lower, ".json") || strings.HasSuffix(lower, ".txt") ||
			strings.HasSuffix(lower, ".conf") || strings.HasSuffix(lower, ".ini") ||
			strings.HasSuffix(lower, ".csv") || strings.HasSuffix(lower, ".toml")
		if err == nil && isText && strings.ContainsRune(body, 0) {
			t.Fatalf("%s with a NUL byte was accepted", name)
		}
	})
}

// FuzzStateTransitions checks that every transition function is total: for any
// pair of state strings it answers, and it never accepts a state it does not
// know.
func FuzzStateTransitions(f *testing.F) {
	f.Add("DRAFT", "ACTIVE")
	f.Add("REJECTED", "DEPLOYABLE")
	f.Add("OPEN", "FIXED")
	f.Add("QUEUED", "RETURNED")
	f.Add("", "")

	f.Fuzz(func(t *testing.T, from, to string) {
		candidateFrom, candidateTo := CandidateState(from), CandidateState(to)
		if err := CheckCandidateTransition(candidateFrom, candidateTo); err == nil {
			if !ValidCandidateState(candidateFrom) || !ValidCandidateState(candidateTo) {
				t.Fatalf("an unknown candidate state was accepted: %q -> %q", from, to)
			}
		}
		issueFrom, issueTo := IssueStatus(from), IssueStatus(to)
		if err := CheckIssueTransition(issueFrom, issueTo); err == nil {
			if !ValidIssueStatus(issueFrom) || !ValidIssueStatus(issueTo) {
				t.Fatalf("an unknown issue state was accepted: %q -> %q", from, to)
			}
		}
		workFrom, workTo := WorkState(from), WorkState(to)
		if err := CheckWorkTransition(workFrom, workTo); err == nil {
			if !ValidWorkState(workFrom) || !ValidWorkState(workTo) {
				t.Fatalf("an unknown work state was accepted: %q -> %q", from, to)
			}
		}
	})
}

// FuzzArtifactKindVocabulary checks that the artifact vocabulary is closed: any
// string answers, and only the five known kinds are accepted.
func FuzzArtifactKindVocabulary(f *testing.F) {
	for _, seed := range []string{
		"CONFIG_BUNDLE", "CODE_REF", "MODEL_REF", "DATA_FIX_REF", "NO_CHANGE",
		"MODEL", "CONFIG", "FILE", "OTHER", "", "config_bundle", "CONFIG_BUNDLE ",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		kind := ArtifactKind(raw)
		valid := ValidArtifactKind(kind)
		known := false
		for _, candidate := range ArtifactKinds() {
			if candidate == kind {
				known = true
			}
		}
		if valid != known {
			t.Fatalf("ValidArtifactKind(%q) = %v, but the vocabulary says %v", raw, valid, known)
		}
	})
}

// FuzzArtifactRequestValidation fuzzes the one-of payload rule: whatever JSON
// arrives, validation must answer without panicking, and anything it accepts
// must carry exactly one payload matching its kind.
func FuzzArtifactRequestValidation(f *testing.F) {
	seeds := []string{
		`{"kind":"CONFIG_BUNDLE","config_bundle":{"bundle":{"a.yaml":"x: 1"}}}`,
		`{"kind":"MODEL_REF","model_ref":{"uri":"s3://x","digest":"sha256:aa"}}`,
		`{"kind":"MODEL_REF","model_ref":{"uri":"s3://x"}}`,
		`{"kind":"CODE_REF","code_ref":{"repository":"r","commit":"c"}}`,
		`{"kind":"DATA_FIX_REF","data_fix_ref":{"patch_id":"p"}}`,
		`{"kind":"NO_CHANGE","no_change":{"reason":"because","disposition":"OTHER"}}`,
		`{"kind":"NO_CHANGE","no_change":{"reason":"because","disposition":"BECAUSE"}}`,
		`{"kind":"MODEL_REF","model_ref":{"uri":"u","digest":"d"},"no_change":{"reason":"r"}}`,
		`{"kind":"MODEL"}`,
		`{}`,
		`{"kind":"CONFIG_BUNDLE","config_bundle":{"bundle":{}}}`,
	}
	for _, seed := range seeds {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		var req ImprovementArtifactRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return
		}
		if err := validateArtifact(req); err != nil {
			return
		}
		// Accepted: the one-of rule must hold.
		supplied := 0
		for _, present := range []bool{
			req.ConfigBundle != nil, req.CodeRef != nil, req.ModelRef != nil,
			req.DataFixRef != nil, req.NoChange != nil,
		} {
			if present {
				supplied++
			}
		}
		if supplied != 1 {
			t.Fatalf("validation accepted kind %q with %d payloads", req.Kind, supplied)
		}
		if !ValidArtifactKind(req.Kind) {
			t.Fatalf("validation accepted unknown kind %q", req.Kind)
		}
		switch req.Kind {
		case ArtifactModelRef:
			if req.ModelRef == nil || strings.TrimSpace(req.ModelRef.URI) == "" || strings.TrimSpace(req.ModelRef.Digest) == "" {
				t.Fatalf("validation accepted a MODEL_REF without uri or digest: %+v", req.ModelRef)
			}
		case ArtifactCodeRef:
			if req.CodeRef == nil || strings.TrimSpace(req.CodeRef.Repository) == "" || strings.TrimSpace(req.CodeRef.Commit) == "" {
				t.Fatalf("validation accepted a CODE_REF without repository or commit: %+v", req.CodeRef)
			}
		case ArtifactNoChange:
			if req.NoChange == nil || strings.TrimSpace(req.NoChange.Reason) == "" {
				t.Fatalf("validation accepted a NO_CHANGE without a reason")
			}
			if !ValidNoChangeDisposition(req.NoChange.Disposition) {
				t.Fatalf("validation accepted unknown disposition %q", req.NoChange.Disposition)
			}
		}
	})
}

// FuzzInstallationNormalization fuzzes the registration rules from mission
// section 40: never panic, and when it succeeds the default installation is one
// of the declared ones and every identity is a UUID.
func FuzzInstallationNormalization(f *testing.F) {
	appID := identity.NewID()
	f.Add(appID, "", 0)
	f.Add(appID, appID, 0)
	f.Add(appID, "", 1)
	f.Add(appID, "", 2)
	f.Add(appID, "not-a-uuid", 1)
	f.Add("not-a-uuid", "", 0)

	f.Fuzz(func(t *testing.T, applicationID, legacy string, count int) {
		if count < 0 || count > 4 {
			return
		}
		specs := make([]InstallationSpec, 0, count)
		for i := 0; i < count; i++ {
			specs = append(specs, InstallationSpec{Name: "site"})
		}

		normalized, defaultID, err := normalizeInstallations(applicationID, legacy, specs)
		if err != nil {
			if normalized != nil || defaultID != "" {
				t.Fatalf("a refused normalization returned %d specs and default %q", len(normalized), defaultID)
			}
			return
		}
		if len(normalized) == 0 {
			t.Fatalf("a successful normalization returned no installations")
		}
		if !identity.Valid(defaultID) {
			t.Fatalf("default installation %q is not a UUID", defaultID)
		}
		found := false
		for _, spec := range normalized {
			if !identity.Valid(spec.InstallationID) {
				t.Fatalf("installation %q is not a UUID", spec.InstallationID)
			}
			if spec.InstallationID == defaultID {
				found = true
			}
		}
		if !found {
			t.Fatalf("default installation %q is not among the declared ones", defaultID)
		}
	})
}
