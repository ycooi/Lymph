package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/projection"
)

// applyRecord projects one canonical ledger record into SQLite.
//
// Everything here is idempotent: replaying a record twice produces the same
// rows. That property is what makes `lymphctl rebuild` safe.
func (e *Engine) applyRecord(tx *sql.Tx, rec ledger.Record) error {
	switch rec.Kind {
	case ledger.KindEvent:
		var p eventPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode event record %d: %w", rec.Sequence, err)
		}
		// The record's own sequence is authoritative for ordering; the payload
		// is written before the append returns, so it cannot contain it.
		p.Event.LedgerSequence = rec.Sequence
		if err := projection.InsertEvent(tx, p.Event); err != nil {
			return err
		}
		if err := projection.UpsertIssue(tx, p.Issue); err != nil {
			return err
		}
		if _, err := projection.RecordOccurrence(tx, p.Occurrence); err != nil {
			return err
		}

	case ledger.KindIssue:
		var p issueStatusPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode issue record %d: %w", rec.Sequence, err)
		}
		if err := projection.SetIssueStatus(tx, p.IssueID, p.Status, p.At); err != nil {
			return err
		}

	case ledger.KindRegistration:
		var p registrationPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode registration record %d: %w", rec.Sequence, err)
		}
		p.Registration.LedgerSequence = rec.Sequence
		installations := p.Installations
		if len(installations) == 0 {
			// A record written before L2.5 carries no installation array. The
			// inference is deterministic and invents nothing (mission 41, 50):
			// the application's own installation if it declared one, otherwise
			// the application identity itself.
			installations = []projection.Installation{{
				ApplicationID:  p.Application.ApplicationID,
				InstallationID: projection.DefaultInstallationID(p.Application),
				Name:           "default",
				Active:         true,
				CreatedAt:      p.Application.CreatedAt,
				UpdatedAt:      p.Application.UpdatedAt,
			}}
		}
		if p.Application.DefaultInstallationID == "" {
			p.Application.DefaultInstallationID = projection.DefaultInstallationID(p.Application)
		}
		if err := projection.UpsertApplication(tx, p.Application); err != nil {
			return err
		}
		for _, installation := range installations {
			if err := projection.UpsertInstallation(tx, installation); err != nil {
				return err
			}
		}
		if err := projection.InsertRegistration(tx, p.Registration); err != nil {
			return err
		}
		for _, j := range p.Junctions {
			if err := projection.UpsertJunction(tx, j); err != nil {
				return err
			}
		}
		for _, f := range p.Families {
			if err := projection.UpsertConfigFamily(tx, f); err != nil {
				return err
			}
		}
		for _, t := range p.Targets {
			if err := projection.RegisterTarget(tx, t); err != nil {
				return err
			}
		}

	case ledger.KindRevision:
		var p revisionPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode revision record %d: %w", rec.Sequence, err)
		}
		if err := projection.InsertRevision(tx, p.Revision, p.Blobs); err != nil {
			return err
		}

	case ledger.KindRef:
		var p refPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode ref record %d: %w", rec.Sequence, err)
		}
		// Legacy ref movements have no installation. Resolve it to the
		// application's default, deterministically, rather than inventing one.
		if p.Ref.InstallationID == "" || p.Reflog.InstallationID == "" {
			app, err := projection.GetApplicationTx(tx, p.Ref.ApplicationID)
			if err != nil {
				return fmt.Errorf("resolve installation for ref record %d: %w", rec.Sequence, err)
			}
			resolved := projection.DefaultInstallationID(app)
			if p.Ref.InstallationID == "" {
				p.Ref.InstallationID = resolved
			}
			if p.Reflog.InstallationID == "" {
				p.Reflog.InstallationID = resolved
			}
		}
		p.Reflog.LedgerSequence = rec.Sequence
		// Replay applies the movement that already happened; the compare-and-swap
		// check belongs to the write path, not to recovery.
		if err := projection.SetRefTx(tx, p.Ref, nil); err != nil {
			return err
		}
		if err := projection.InsertReflog(tx, p.Reflog); err != nil {
			return err
		}

	case ledger.KindCandidate:
		var p candidatePayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode candidate record %d: %w", rec.Sequence, err)
		}
		if err := projection.InsertCandidate(tx, p.Candidate); err != nil {
			return err
		}

	case ledger.KindImprovementResult:
		// One record reconstructs the whole worker return: result, artifacts,
		// any candidate the return created, and the work item's RETURNED state
		// (mission sections 18, 26). No worker logic and no validator runs here.
		var p improvementResultPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode improvement result record %d: %w", rec.Sequence, err)
		}
		if err := projection.InsertImprovementResult(tx, p.Result); err != nil {
			return err
		}
		for _, artifact := range p.Artifacts {
			if err := projection.InsertImprovementArtifact(tx, artifact); err != nil {
				return err
			}
		}
		for _, candidate := range p.Candidates {
			if err := projection.InsertCandidate(tx, candidate); err != nil {
				return err
			}
		}
		if err := projection.InsertWorkItem(tx, p.WorkItem); err != nil {
			return err
		}

	case ledger.KindValidation:
		var p validationPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode validation record %d: %w", rec.Sequence, err)
		}
		if err := projection.InsertValidation(tx, p.Validation); err != nil {
			return err
		}

	case ledger.KindApproval:
		var p approvalPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode approval record %d: %w", rec.Sequence, err)
		}
		if err := projection.InsertApproval(tx, p.Approval); err != nil {
			return err
		}

	case ledger.KindWorkItem:
		var p workPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode work record %d: %w", rec.Sequence, err)
		}
		if err := projection.InsertWorkItem(tx, p.WorkItem); err != nil {
			return err
		}

	case ledger.KindAudit:
		var p auditPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode audit record %d: %w", rec.Sequence, err)
		}
		p.Audit.LedgerSequence = rec.Sequence
		if err := projection.InsertAuditTx(tx, p.Audit); err != nil {
			return err
		}

	case ledger.KindUpdateApplicationResult:
		// A node's applied/rejected report is history: it comes back from the
		// ledger, and the delivery disposition follows it (mission section 64).
		var p updateApplicationResultPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode application result record %d: %w", rec.Sequence, err)
		}
		if err := projection.InsertApplicationResult(tx, p.Result); err != nil {
			return err
		}
		state := projection.UpdateApplied
		if p.Result.Result == projection.ResultRejected {
			state = projection.UpdateRejected
		}
		if err := projection.UpsertUpdateDisposition(tx, projection.UpdateDisposition{
			ApplicationID:  p.Result.ApplicationID,
			InstallationID: p.Result.InstallationID,
			ConfigFamilyID: p.Result.ConfigFamilyID,
			RevisionID:     p.Result.RevisionID,
			ContentHash:    p.Result.ContentHash,
			ApprovalID:     p.Result.ApprovalID,
			State:          state,
			ReasonCode:     p.Result.ReasonCode,
			ObservedAt:     p.Result.CreatedAt,
			UpdatedAt:      p.Result.CreatedAt,
		}); err != nil {
			return err
		}

	case ledger.KindUpdateWithdrawn:
		var p updateWithdrawalPayload
		if err := json.Unmarshal(rec.Payload, &p); err != nil {
			return fmt.Errorf("decode withdrawal record %d: %w", rec.Sequence, err)
		}
		existing, err := projection.GetUpdateDispositionTx(tx, p.ApplicationID, p.InstallationID, p.RevisionID)
		disposition := projection.UpdateDisposition{
			ApplicationID:  p.ApplicationID,
			InstallationID: p.InstallationID,
			ConfigFamilyID: p.ConfigFamilyID,
			RevisionID:     p.RevisionID,
			State:          projection.UpdateWithdrawn,
			ReasonCode:     "OPERATOR_WITHDRAWN",
			UpdatedAt:      p.At,
		}
		if err == nil {
			disposition.ContentHash = existing.ContentHash
			disposition.ApprovalID = existing.ApprovalID
			disposition.FetchedAt = existing.FetchedAt
			disposition.ObservedAt = existing.ObservedAt
		}
		if err := projection.UpsertUpdateDisposition(tx, disposition); err != nil {
			return err
		}

	default:
		// An unknown kind is not fatal for projection: canonical truth is still
		// intact and the record is indexed below. It means this binary is older
		// than the writer, which the operator should know about.
		e.log.Warn("unknown ledger record kind ignored by projection", "kind", string(rec.Kind), "sequence", rec.Sequence)
	}

	return projection.IndexLedgerRecordTx(tx, rec)
}

// catchUp replays ledger records newer than `from` into the projection.
// It returns how many records it applied.
func (e *Engine) catchUp(from uint64) (int, error) {
	const batchSize = 256
	applied := 0
	pending := make([]ledger.Record, 0, batchSize)

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		batch := pending
		err := e.db.Tx(func(tx *sql.Tx) error {
			for _, rec := range batch {
				if err := e.applyRecord(tx, rec); err != nil {
					return err
				}
			}
			last := batch[len(batch)-1]
			if err := projection.SetMetaTx(tx, projection.MetaLedgerSeq, fmt.Sprint(last.Sequence)); err != nil {
				return err
			}
			if err := projection.SetMetaTx(tx, projection.MetaLedgerHash, last.RecordHash); err != nil {
				return err
			}
			return projection.SetMetaTx(tx, projection.MetaBuiltAt, time.Now().UTC().Format(time.RFC3339Nano))
		})
		if err != nil {
			return err
		}
		applied += len(batch)
		pending = pending[:0]
		return nil
	}

	err := e.ledger.ForEach(func(rec ledger.Record) error {
		if rec.Sequence <= from {
			return nil
		}
		pending = append(pending, rec)
		if len(pending) >= batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return applied, err
	}
	if err := flush(); err != nil {
		return applied, err
	}
	return applied, nil
}

// Rebuild discards the projection and replays the whole ledger (section 88).
//
// The canonical ledger and object store are untouched. This is the command to
// run after SQLite corruption: preserve the corrupt file for diagnosis, then
// rebuild from truth.
func (e *Engine) Rebuild() (int, error) {
	if err := e.db.Reset(); err != nil {
		return 0, err
	}
	if err := e.db.SetMeta(projection.MetaInstanceUUID, e.ident.InstanceUUID); err != nil {
		return 0, err
	}
	applied, err := e.catchUp(0)
	if err != nil {
		return applied, err
	}
	rebuilds, _, err := e.db.Meta(projection.MetaRebuilds)
	if err != nil {
		return applied, err
	}
	next := 1
	if rebuilds != "" {
		fmt.Sscanf(rebuilds, "%d", &next)
		next++
	}
	if err := e.db.SetMeta(projection.MetaRebuilds, fmt.Sprint(next)); err != nil {
		return applied, err
	}
	return applied, nil
}

// EnsureProjected makes the projection match the ledger head, replaying only
// what is missing (section 58).
func (e *Engine) EnsureProjected() (int, error) {
	seq, hash, _, err := e.db.LedgerCheckpoint()
	if err != nil {
		return 0, err
	}
	if seq == e.ledger.LastSequence() && hash == e.ledgerHeadHash() {
		return 0, nil
	}
	return e.catchUp(seq)
}

func (e *Engine) ledgerHeadHash() string {
	return e.ledger.HeadHash()
}
