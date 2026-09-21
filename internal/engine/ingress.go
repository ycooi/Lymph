package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ycooi/Lymph/internal/fingerprint"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/internal/protocol"
)

// ErrUnknownApplication is returned when an event arrives from an application
// that never registered (section 7).
var ErrUnknownApplication = errors.New("unknown application: register before emitting")

// ErrUnknownJunction is returned when an event names a junction that the
// application did not declare (section 8).
var ErrUnknownJunction = errors.New("unknown junction")

// EmitRequest is one feedback submission (section 64).
type EmitRequest struct {
	Event            protocol.Event    `json:"-"`
	ProducerInstance string            `json:"producer_instance,omitempty"`
	InstallationID   string            `json:"installation_id,omitempty"`
	Durability       ledger.Durability `json:"durability,omitempty"`
	// Fingerprint lets a junction supply its own grouping key (section 35).
	// Empty means the generic fallback is used.
	Fingerprint string `json:"fingerprint,omitempty"`
	// SessionID is the session established by the handshake. Empty means a
	// legacy sessionless client, which stays accepted during this stage
	// (mission section 22).
	SessionID string `json:"-"`
	// Replay marks an event resent from a client spool. A replay may carry the
	// process identity of the process that produced it, which is not
	// necessarily the process that is replaying it (mission sections 32, 63).
	Replay bool `json:"-"`
}

// EmitResponse acknowledges an accepted event.
type EmitResponse struct {
	EventID        string `json:"event_id"`
	Accepted       bool   `json:"accepted"`
	Duplicate      bool   `json:"duplicate"`
	Durability     string `json:"durability"`
	Fingerprint    string `json:"fingerprint"`
	IssueID        string `json:"issue_id,omitempty"`
	IssueCreated   bool   `json:"issue_created"`
	OccurrenceNo   int    `json:"occurrence_count"`
	LedgerSequence uint64 `json:"ledger_sequence"`
}

// Emit accepts one LymphEvent (section 10, section 64).
//
// Idempotency: re-sending the same event id returns the original
// acknowledgement and creates nothing (section 68), which is what makes the
// client spool safe (section 67).
func (e *Engine) Emit(ctx context.Context, req EmitRequest) (EmitResponse, error) {
	ev := req.Event
	if ev.SpecVersion == "" {
		ev.SpecVersion = protocol.SpecVersion
	}
	if ev.ID == "" {
		ev.ID = identity.NewID()
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	if err := ev.Validate(); err != nil {
		return EmitResponse{}, fmt.Errorf("invalid event: %w", err)
	}

	applicationID, junctionToken, err := protocol.ParseEventSource(ev.Source)
	if err != nil {
		return EmitResponse{}, err
	}
	feedbackType, err := protocol.FeedbackTypeOf(ev.Type)
	if err != nil {
		return EmitResponse{}, err
	}
	data, err := ev.DecodeData()
	if err != nil {
		return EmitResponse{}, err
	}

	durability := req.Durability
	if durability == "" {
		durability = e.opts.DefaultDurability
	}
	if durability != ledger.Async && durability != ledger.Durable {
		return EmitResponse{}, fmt.Errorf("unknown durability %q", durability)
	}

	now := time.Now().UTC()
	var resp EmitResponse

	err = e.withTx(func(tx *sql.Tx) error {
		// Idempotency first: a duplicated send must not touch anything.
		duplicate, err := projection.EventExists(tx, ev.ID)
		if err != nil {
			return err
		}
		if duplicate {
			existing, err := projection.GetEventTx(tx, ev.ID)
			if err != nil {
				return err
			}
			issue, _ := projection.GetIssueByFingerprintTx(tx, existing.Fingerprint)
			resp = EmitResponse{
				EventID:        existing.EventID,
				Accepted:       true,
				Duplicate:      true,
				Durability:     existing.Durability,
				Fingerprint:    existing.Fingerprint,
				IssueID:        issue.IssueID,
				OccurrenceNo:   issue.OccurrenceCount,
				LedgerSequence: existing.LedgerSequence,
			}
			return nil
		}

		// Session validation, when the client presented one (mission sections
		// 20, 21, 61, 62, 63).
		//
		// A normal event must agree with its session on application,
		// installation and process. A replay must agree on application and
		// installation only: the event carries the identity of the process that
		// produced it, which is historical truth and is never rewritten.
		if req.SessionID != "" {
			session, err := e.SessionLookup(tx, req.SessionID, now)
			if err != nil {
				if errors.Is(err, projection.ErrNotFound) {
					return fmt.Errorf("%w: %s", ErrSessionUnknown, req.SessionID)
				}
				return err
			}
			if err := sessionAcceptsEvent(session, &req, applicationID); err != nil {
				e.auditTx(tx, projection.AuditEvent{
					Action:        "session.identity_mismatch",
					ApplicationID: session.ApplicationID,
					SubjectKind:   "session",
					SubjectID:     session.SessionID,
					Detail:        fmt.Sprintf(`{"event_id":%q,"detail":%q}`, ev.ID, err.Error()),
				})
				return err
			}
			if req.Replay {
				e.spoolReplay()
			}
		} else {
			// Legacy sessionless clients keep working during this stage, but
			// they are counted so the migration is visible (mission section 89).
			e.sessionlessEvent()
		}

		app, err := projection.GetApplicationTx(tx, applicationID)
		if err != nil {
			if errors.Is(err, projection.ErrNotFound) {
				return fmt.Errorf("%w: application %s", ErrUnknownApplication, applicationID)
			}
			return err
		}
		junction, err := resolveJunctionTx(tx, app.ApplicationID, junctionToken)
		if err != nil {
			return err
		}

		fp := req.Fingerprint
		if fp == "" {
			fp = fingerprint.Generic(app.ApplicationID, junction.JunctionID, string(feedbackType),
				data.ReasonCode, fingerprintSource(data))
		}

		issue, issueCreated, err := upsertIssueForEvent(tx, issueInput{
			Fingerprint:         fp,
			ApplicationID:       app.ApplicationID,
			JunctionID:          junction.JunctionID,
			FeedbackType:        string(feedbackType),
			ReasonCode:          data.ReasonCode,
			Pattern:             fingerprint.Pattern(fingerprintSource(data)),
			ControllingRevision: data.ConfigRevision,
			Producer:            req.ProducerInstance,
			EventID:             ev.ID,
			At:                  now,
		})
		if err != nil {
			return err
		}

		row := projection.Event{
			EventID:          ev.ID,
			ApplicationID:    app.ApplicationID,
			JunctionID:       junction.JunctionID,
			InstallationID:   installationOf(req.InstallationID, app.InstallationID),
			Type:             ev.Type,
			Source:           ev.Source,
			Subject:          subjectOr(ev.Subject, junction.Name),
			FeedbackType:     string(feedbackType),
			ReasonCode:       data.ReasonCode,
			Fingerprint:      fp,
			ProducerInstance: req.ProducerInstance,
			ContractRevision: data.ContractRevision,
			ConfigRevision:   data.ConfigRevision,
			ConfigHash:       data.ConfigHash,
			InputRef:         data.InputRef,
			ReplayRef:        data.ReplayRef,
			// Operational context: which session carried this event, and whether
			// it was a replay. Never part of a fingerprint (mission section 120).
			SessionID:  req.SessionID,
			Durability: string(durability),
			Data:       string(ev.Data),
			OccurredAt: ev.Time,
			ReceivedAt: now,
		}
		if req.Replay {
			row.ReplayedBySessionID = req.SessionID
		}

		rec, err := e.ledger.Append(ledger.KindEvent, ev.ID, eventPayload{
			Event:        row,
			Issue:        issue,
			IssueCreated: issueCreated,
			Occurrence: projection.Occurrence{
				IssueID:  issue.IssueID,
				EventID:  ev.ID,
				SeenAt:   now,
				Producer: req.ProducerInstance,
			},
		}, durability)
		if err != nil {
			return err
		}

		row.LedgerSequence = rec.Sequence
		if err := projection.InsertEvent(tx, row); err != nil {
			return err
		}
		if err := projection.UpsertIssue(tx, issue); err != nil {
			return err
		}
		if err := projection.IndexLedgerRecordTx(tx, rec); err != nil {
			return err
		}
		if err := checkpoint(tx, rec); err != nil {
			return err
		}

		resp = EmitResponse{
			EventID:        ev.ID,
			Accepted:       true,
			Durability:     string(durability),
			Fingerprint:    fp,
			IssueID:        issue.IssueID,
			IssueCreated:   issueCreated,
			OccurrenceNo:   issue.OccurrenceCount,
			LedgerSequence: rec.Sequence,
		}
		return nil
	})
	if err != nil {
		return EmitResponse{}, err
	}

	if resp.Accepted && !resp.Duplicate {
		e.log.Info("event accepted",
			"event", resp.EventID, "issue", resp.IssueID, "new_issue", resp.IssueCreated,
			"occurrences", resp.OccurrenceNo, "sequence", resp.LedgerSequence)
	}
	return resp, nil
}

type issueInput struct {
	Fingerprint         string
	ApplicationID       string
	JunctionID          string
	FeedbackType        string
	ReasonCode          string
	Pattern             string
	ControllingRevision string
	Producer            string
	EventID             string
	At                  time.Time
}

// upsertIssueForEvent groups an event into its recurring issue and returns the
// issue row exactly as it will be recorded in the canonical payload
// (section 34, section 36).
func upsertIssueForEvent(tx *sql.Tx, in issueInput) (projection.Issue, bool, error) {
	issue, err := projection.GetIssueByFingerprintTx(tx, in.Fingerprint)
	created := false
	switch {
	case errors.Is(err, projection.ErrNotFound):
		created = true
		issue = projection.Issue{
			IssueID:             identity.NewID(),
			ApplicationID:       in.ApplicationID,
			JunctionID:          in.JunctionID,
			FeedbackType:        in.FeedbackType,
			ReasonCode:          in.ReasonCode,
			Fingerprint:         in.Fingerprint,
			Pattern:             in.Pattern,
			Status:              string(IssueOpen),
			FirstSeen:           in.At,
			LastSeen:            in.At,
			CreatedAt:           in.At,
			UpdatedAt:           in.At,
			ControllingRevision: in.ControllingRevision,
		}
	case err != nil:
		return issue, false, err
	default:
		// Arrival order is authoritative (ledger sequence); the client-supplied
		// event time may jump backwards. Keeping min/max stops a clock skew
		// from producing an issue whose last occurrence precedes its first.
		if in.At.Before(issue.FirstSeen) {
			issue.FirstSeen = in.At
		}
		if in.At.After(issue.LastSeen) {
			issue.LastSeen = in.At
		}
		issue.UpdatedAt = in.At
		if in.ControllingRevision != "" {
			issue.ControllingRevision = in.ControllingRevision
		}
		// A new occurrence on a fixed or ignored issue reopens it: reality has
		// contradicted the earlier decision.
		if issue.Status == string(IssueFixed) || issue.Status == string(IssueIgnored) {
			issue.Status = string(IssueReopened)
		}
	}

	// The occurrence row is what makes counters exact; recomputing from it
	// instead of incrementing keeps replay idempotent.
	if _, err := projection.RecordOccurrence(tx, projection.Occurrence{
		IssueID:  issue.IssueID,
		EventID:  in.EventID,
		SeenAt:   in.At,
		Producer: in.Producer,
	}); err != nil {
		return issue, false, err
	}
	if err := projection.UpsertIssue(tx, issue); err != nil {
		return issue, false, err
	}
	if err := projection.RecomputeIssueCounters(tx, issue.IssueID, in.At); err != nil {
		return issue, false, err
	}
	finalised, err := projection.GetIssueTx(tx, issue.IssueID)
	if err != nil {
		return issue, false, err
	}
	return finalised, created, nil
}

// resolveJunctionTx accepts either the stable junction UUID or the junction
// name. Names are metadata (section 4); the resolved identity is what gets
// stored on the event.
func resolveJunctionTx(tx *sql.Tx, applicationID, token string) (projection.Junction, error) {
	if identity.Valid(token) {
		j, err := projection.GetJunctionTx(tx, token)
		if err != nil {
			if errors.Is(err, projection.ErrNotFound) {
				return j, fmt.Errorf("%w: %s", ErrUnknownJunction, token)
			}
			return j, err
		}
		if j.ApplicationID != applicationID {
			return j, fmt.Errorf("%w: junction %s belongs to application %s", ErrUnknownJunction, token, j.ApplicationID)
		}
		return j, nil
	}
	j, err := projection.GetJunctionByNameTx(tx, applicationID, token)
	if err != nil {
		if errors.Is(err, projection.ErrNotFound) {
			return j, fmt.Errorf("%w: %s/%s", ErrUnknownJunction, applicationID, token)
		}
		return j, err
	}
	return j, nil
}

func fingerprintSource(d protocol.EventData) string {
	if len(d.Payload) > 0 {
		return string(d.Payload)
	}
	return d.InputRef
}

func subjectOr(subject, fallback string) string {
	if subject != "" {
		return subject
	}
	return fallback
}

// installationOf prefers the installation named on the event, falling back to
// the one recorded at registration (section 79).
func installationOf(fromEvent, fromRegistration string) string {
	if fromEvent != "" {
		return fromEvent
	}
	return fromRegistration
}

// checkpoint advances the projection's position in the ledger.
func checkpoint(tx *sql.Tx, rec ledger.Record) error {
	if err := projection.SetMetaTx(tx, projection.MetaLedgerSeq, fmt.Sprint(rec.Sequence)); err != nil {
		return err
	}
	if err := projection.SetMetaTx(tx, projection.MetaLedgerHash, rec.RecordHash); err != nil {
		return err
	}
	return projection.SetMetaTx(tx, projection.MetaBuiltAt, time.Now().UTC().Format(time.RFC3339Nano))
}

// NewEvent is a convenience constructor used by the SDK and lymphctl.
func NewEvent(source, eventType, subject string, data protocol.EventData) (protocol.Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return protocol.Event{}, err
	}
	return protocol.Event{
		SpecVersion:     protocol.SpecVersion,
		ID:              identity.NewID(),
		Source:          source,
		Type:            eventType,
		Subject:         subject,
		Time:            time.Now().UTC(),
		DataContentType: "application/json",
		Data:            raw,
	}, nil
}
