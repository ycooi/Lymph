// Package protocol defines the wire types shared by lymphd, the Go SDK and
// lymphctl.
//
// Design references:
//   - section 10: LymphEvent - CloudEvents 1.0 outer envelope, payload evolves independently
//   - section 63: HTTP+JSON over a Unix domain socket
//
// The envelope is implemented directly rather than through the CloudEvents Go
// SDK: the wire form is exactly the CloudEvents 1.0 JSON format (specversion,
// id, source, type, subject, time, datacontenttype, data), and keeping the
// encoder local avoids pulling a large dependency tree into the daemon
// (section 92: dependencies should stay small).
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SpecVersion is the CloudEvents specification version Lymph emits.
const SpecVersion = "1.0"

// SourcePrefix is the URI scheme used for event sources (section 10).
const SourcePrefix = "lymph://"

// FeedbackType is the universal feedback taxonomy (section 9).
type FeedbackType string

// The initial universal vocabulary from section 9.
const (
	FeedbackUnknown           FeedbackType = "UNKNOWN"
	FeedbackAmbiguous         FeedbackType = "AMBIGUOUS"
	FeedbackConflict          FeedbackType = "CONFLICT"
	FeedbackLowConfidence     FeedbackType = "LOW_CONFIDENCE"
	FeedbackOutOfContract     FeedbackType = "OUT_OF_CONTRACT"
	FeedbackQualityFailure    FeedbackType = "QUALITY_FAILURE"
	FeedbackRegression        FeedbackType = "REGRESSION"
	FeedbackSchemaDrift       FeedbackType = "SCHEMA_DRIFT"
	FeedbackPerformanceDrift  FeedbackType = "PERFORMANCE_DRIFT"
	FeedbackHumanCorrection   FeedbackType = "HUMAN_CORRECTION"
	FeedbackRuntimeFailure    FeedbackType = "RUNTIME_FAILURE"
	FeedbackConfigDrift       FeedbackType = "CONFIG_DRIFT"
	FeedbackDeploymentFailure FeedbackType = "DEPLOYMENT_FAILURE"
)

var feedbackTypes = map[FeedbackType]struct{}{
	FeedbackUnknown: {}, FeedbackAmbiguous: {}, FeedbackConflict: {},
	FeedbackLowConfidence: {}, FeedbackOutOfContract: {}, FeedbackQualityFailure: {},
	FeedbackRegression: {}, FeedbackSchemaDrift: {}, FeedbackPerformanceDrift: {},
	FeedbackHumanCorrection: {}, FeedbackRuntimeFailure: {}, FeedbackConfigDrift: {},
	FeedbackDeploymentFailure: {},
}

// Valid reports whether t is part of the universal taxonomy.
func (t FeedbackType) Valid() bool {
	_, ok := feedbackTypes[t]
	return ok
}

// FeedbackTypes lists the taxonomy in stable order.
func FeedbackTypes() []FeedbackType {
	return []FeedbackType{
		FeedbackUnknown, FeedbackAmbiguous, FeedbackConflict, FeedbackLowConfidence,
		FeedbackOutOfContract, FeedbackQualityFailure, FeedbackRegression,
		FeedbackSchemaDrift, FeedbackPerformanceDrift, FeedbackHumanCorrection,
		FeedbackRuntimeFailure, FeedbackConfigDrift, FeedbackDeploymentFailure,
	}
}

// EventData is the data member of a Lymph feedback event (section 10).
//
// Payload is deliberately opaque: Lymph routes and versions configuration, it
// never interprets domain meaning (section 2, section 100).
type EventData struct {
	FeedbackType     FeedbackType    `json:"feedback_type"`
	ReasonCode       string          `json:"reason_code,omitempty"`
	ConfigRevision   string          `json:"config_revision,omitempty"`
	ConfigHash       string          `json:"config_hash,omitempty"`
	ContractRevision string          `json:"contract_revision,omitempty"`
	InputRef         string          `json:"input_ref,omitempty"`
	ReplayRef        string          `json:"replay_ref,omitempty"`
	Payload          json.RawMessage `json:"payload,omitempty"`
}

// Event is a CloudEvents 1.0 envelope carrying a LymphEvent (section 10).
type Event struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	Subject         string          `json:"subject,omitempty"`
	Time            time.Time       `json:"time"`
	DataContentType string          `json:"datacontenttype,omitempty"`
	Data            json.RawMessage `json:"data"`
}

// EventSource builds the canonical CloudEvents source URI for an application
// and junction pair.
func EventSource(applicationID, junctionID string) string {
	return SourcePrefix + applicationID + "/" + junctionID
}

// ParseEventSource splits a source URI back into its parts.
func ParseEventSource(src string) (applicationID, junctionID string, err error) {
	rest, ok := strings.CutPrefix(src, SourcePrefix)
	if !ok {
		return "", "", fmt.Errorf("source %q does not start with %q", src, SourcePrefix)
	}
	app, junc, ok := strings.Cut(rest, "/")
	if !ok || app == "" || junc == "" {
		return "", "", fmt.Errorf("source %q is not of the form lymph://<application>/<junction>", src)
	}
	return app, junc, nil
}

// FeedbackTypeOf extracts the feedback type from an event type string of the
// form "lymph.feedback.<type>.v1".
func FeedbackTypeOf(eventType string) (FeedbackType, error) {
	parts := strings.Split(eventType, ".")
	if len(parts) != 4 || parts[0] != "lymph" || parts[1] != "feedback" || parts[3] != "v1" {
		return "", fmt.Errorf("event type %q is not of the form lymph.feedback.<type>.v1", eventType)
	}
	ft := FeedbackType(strings.ToUpper(parts[2]))
	if !ft.Valid() {
		return "", fmt.Errorf("unknown feedback type %q", parts[2])
	}
	return ft, nil
}

// FeedbackEventType builds the reverse of FeedbackTypeOf.
func FeedbackEventType(t FeedbackType) string {
	return "lymph.feedback." + strings.ToLower(string(t)) + ".v1"
}

// DecodeData unmarshals the event data member.
func (e Event) DecodeData() (EventData, error) {
	var d EventData
	if len(e.Data) == 0 {
		return d, errors.New("event has no data member")
	}
	if err := json.Unmarshal(e.Data, &d); err != nil {
		return d, fmt.Errorf("decode event data: %w", err)
	}
	return d, nil
}

// Validate checks the envelope invariants Lymph relies on.
func (e Event) Validate() error {
	switch {
	case e.SpecVersion != SpecVersion:
		return fmt.Errorf("unsupported specversion %q", e.SpecVersion)
	case e.ID == "":
		return errors.New("event id is required")
	case e.Source == "":
		return errors.New("event source is required")
	case e.Type == "":
		return errors.New("event type is required")
	case e.Time.IsZero():
		return errors.New("event time is required")
	}
	if _, _, err := ParseEventSource(e.Source); err != nil {
		return err
	}
	if _, err := FeedbackTypeOf(e.Type); err != nil {
		return err
	}
	d, err := e.DecodeData()
	if err != nil {
		return err
	}
	if d.FeedbackType == "" {
		return errors.New("event data is missing feedback_type")
	}
	if !d.FeedbackType.Valid() {
		return fmt.Errorf("unknown feedback_type %q", d.FeedbackType)
	}
	return nil
}
