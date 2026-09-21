package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/ledger"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/pkg/client"
)

// human renders a value for a terminal: aligned columns, short identifiers,
// no decoration that would not survive a copy-paste.
func human(v any) string {
	switch value := v.(type) {
	case engine.Status:
		return statusBlock(value)
	case ledger.VerifyReport:
		return fmt.Sprintf("ledger OK: %d records in %d segments, sequences %d..%d, head %s",
			value.Records, value.Segments, value.FirstSeq, value.LastSeq, shortHash(value.HeadHash))
	case client.EmitResponse:
		state := "accepted"
		if value.Duplicate {
			state = "duplicate (already recorded)"
		}
		if value.Spooled {
			state = "spooled locally"
		}
		return fmt.Sprintf("%s\nevent    %s\nissue    %s%s\ncount    %d\nsequence %d\nfingerprint %s",
			state, value.EventID, value.IssueID, newTag(value.IssueCreated), value.OccurrenceNo,
			value.LedgerSequence, shortHash(value.Fingerprint))
	case engine.Registration:
		var b strings.Builder
		fmt.Fprintf(&b, "application %s (%s)\n", value.Application.Name, value.Application.ApplicationID)
		fmt.Fprintf(&b, "revision     %d%s\n", value.Application.RegistrationRevision, changedTag(value.Changed))
		fmt.Fprintf(&b, "manifest     %s\n", shortHash(value.Registration.ContentHash))
		if len(value.JunctionIDs) > 0 {
			b.WriteString("junctions\n")
			for _, name := range sortedKeys(value.JunctionIDs) {
				fmt.Fprintf(&b, "  %-32s %s\n", name, value.JunctionIDs[name])
			}
		}
		if len(value.FamilyIDs) > 0 {
			b.WriteString("config families\n")
			for _, name := range sortedKeys(value.FamilyIDs) {
				fmt.Fprintf(&b, "  %-32s %s\n", name, value.FamilyIDs[name])
			}
		}
		return strings.TrimRight(b.String(), "\n")
	case engine.PromotionResult:
		return fmt.Sprintf("promoted to r%d  %s\ncandidate %s -> %s\ntree      %s\nrun `lymphctl refs --app %s --family %s` for the resulting pointers",
			value.Revision.Sequence, value.Revision.RevisionID, value.Candidate.CandidateID,
			value.Candidate.State, shortHash(value.Revision.RootTreeHash),
			shortID(value.Revision.ApplicationID), shortID(value.Revision.ConfigFamilyID))
	case engine.DiffResult:
		if len(value.Files) == 0 {
			return fmt.Sprintf("r%d and r%d have identical content",
				value.From.Sequence, value.To.Sequence)
		}
		var b strings.Builder
		fmt.Fprintf(&b, "r%d -> r%d\n", value.From.Sequence, value.To.Sequence)
		for _, file := range value.Files {
			fmt.Fprintf(&b, "%-9s %s\n", file.Status, file.Path)
		}
		for _, file := range value.Files {
			if file.Patch != "" {
				b.WriteString("\n")
				b.WriteString(file.Patch)
			}
		}
		for _, note := range value.Notes {
			fmt.Fprintf(&b, "note: %s\n", note)
		}
		return strings.TrimRight(b.String(), "\n")
	case engine.ReturnImprovementResponse:
		var b strings.Builder
		if value.Idempotent {
			b.WriteString("(already returned; this is the stored result)\n")
		}
		fmt.Fprintf(&b, "result    %s  attempt %d by %s\nwork      %s -> %s\n",
			value.Result.ResultID, value.Result.Attempt, value.Result.Worker,
			value.WorkItem.WorkID, value.WorkItem.State)
		if value.Result.Summary != "" {
			fmt.Fprintf(&b, "summary   %s\n", value.Result.Summary)
		}
		b.WriteString("artifacts\n")
		for _, artifact := range value.Artifacts {
			fmt.Fprintf(&b, "  %-14s %s\n", artifact.Kind, artifact.ArtifactID)
		}
		if len(value.Candidates) > 0 {
			b.WriteString("candidates\n")
			for _, candidate := range value.Candidates {
				fmt.Fprintf(&b, "  %-14s %s (state %s)\n", candidate.ConfigFamilyID[:8]+"…",
					candidate.CandidateID, candidate.State)
			}
		}
		return strings.TrimRight(b.String(), "\n")
	case resultWithArtifacts:
		var b strings.Builder
		fmt.Fprintf(&b, "result    %s\nwork      %s\nworker    %s (attempt %d)\napplication %s\nsummary   %s\ncreated   %s\n",
			value.Result.ResultID, value.Result.WorkItemID, value.Result.Worker, value.Result.Attempt,
			value.Result.ApplicationID, value.Result.Summary, stamp(value.Result.CreatedAt))
		if len(value.Result.IssueIDs) > 0 {
			fmt.Fprintf(&b, "issues    %s\n", strings.Join(value.Result.IssueIDs, ", "))
		}
		b.WriteString("artifacts\n")
		for _, artifact := range value.Artifacts {
			fmt.Fprintf(&b, "  %-14s %s\n    %s\n", artifact.Kind, artifact.ArtifactID, truncate(artifact.Payload, 120))
		}
		return strings.TrimRight(b.String(), "\n")
	case []projection.ImprovementResult:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"RESULT", "WORK", "WORKER", "ATT", "SUMMARY", "CREATED"})
		for _, result := range value {
			rows = append(rows, []string{shortID(result.ResultID), shortID(result.WorkItemID),
				result.Worker, fmt.Sprint(result.Attempt), truncate(result.Summary, 32), stamp(result.CreatedAt)})
		}
		return table(rows)
	case []projection.ImprovementArtifact:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"ARTIFACT", "KIND", "CANDIDATE", "CONTENT", "PAYLOAD"})
		for _, artifact := range value {
			rows = append(rows, []string{shortID(artifact.ArtifactID), artifact.Kind,
				shortID(artifact.CandidateID), shortHash(artifact.ContentHash), truncate(artifact.Payload, 60)})
		}
		return table(rows)
	case projection.ImprovementArtifact:
		return fmt.Sprintf("artifact  %s\nkind      %s\nresult    %s\napplication %s\nfamily    %s\ninstallation %s\ncandidate %s\ncontent   %s\ncreated   %s\npayload\n  %s",
			value.ArtifactID, value.Kind, value.ResultID, value.ApplicationID, value.ConfigFamilyID,
			value.InstallationID, value.CandidateID, value.ContentHash, stamp(value.CreatedAt),
			truncate(value.Payload, 400))
	case []projection.Installation:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"INSTALLATION", "NAME", "ENVIRONMENT", "ACTIVE", "APPLICATION"})
		for _, installation := range value {
			rows = append(rows, []string{shortID(installation.InstallationID), installation.Name,
				installation.Environment, boolMark(installation.Active), shortID(installation.ApplicationID)})
		}
		return table(rows)
	case []projection.Session:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"SESSION", "PROCESS", "CLIENT", "PROTOCOL", "STATE", "LAST SEEN", "PEER"})
		for _, session := range value {
			clientLabel := session.ClientName
			if session.ClientVersion != "" {
				clientLabel = session.ClientName + " " + session.ClientVersion
			}
			peer := "-"
			if session.PeerUID >= 0 {
				peer = fmt.Sprintf("uid %d gid %d", session.PeerUID, session.PeerGID)
			}
			rows = append(rows, []string{shortID(session.SessionID), shortID(session.ProcessID),
				truncate(clientLabel, 22), session.ProtocolVersion, session.State,
				stamp(session.LastSeenAt), peer})
		}
		return table(rows)
	case projection.Session:
		peer := "not available on this platform"
		if value.PeerUID >= 0 {
			peer = fmt.Sprintf("uid %d gid %d", value.PeerUID, value.PeerGID)
		}
		closed := "-"
		if !value.ClosedAt.IsZero() {
			closed = stamp(value.ClosedAt)
		}
		return fmt.Sprintf("session     %s\napplication %s\ninstallation %s\nprocess     %s\nclient      %s %s\nprotocol    %s\npid/host    %d %s\npeer        %s\nstate       %s\nconnected   %s\nlast seen   %s\nclosed      %s",
			value.SessionID, value.ApplicationID, value.InstallationID, value.ProcessID,
			value.ClientName, value.ClientVersion, value.ProtocolVersion,
			value.PID, value.Hostname, peer, value.State,
			stamp(value.ConnectedAt), stamp(value.LastSeenAt), closed)
	case client.HelloResponse:
		var b strings.Builder
		if value.Warnings != nil {
			for _, warning := range value.Warnings {
				fmt.Fprintf(&b, "warning: %s\n", warning)
			}
		}
		fmt.Fprintf(&b, "accepted    %v\nsession     %s\nprotocol    %s\nserver      %s\nlymph       %s\napplication %s\ninstallation %s\nprocess     %s\nregistered  %v\ncapabilities %s",
			value.Accepted, value.SessionID, value.ProtocolVersion, value.ServerVersion,
			value.LymphInstanceID, value.ApplicationID, value.InstallationID, value.ProcessID,
			value.Registered, strings.Join(value.Capabilities, ", "))
		return b.String()
	case []projection.Application:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"NAME", "APPLICATION_ID", "TYPE", "OWNER", "REV"})
		for _, a := range value {
			rows = append(rows, []string{a.Name, a.ApplicationID, a.AppType, a.Owner, fmt.Sprint(a.RegistrationRevision)})
		}
		return table(rows)
	case []projection.Junction:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"JUNCTION", "APPLICATION", "WORKFLOW", "FAMILY", "FEEDBACK TYPES"})
		for _, j := range value {
			rows = append(rows, []string{j.Name, j.ApplicationID, j.WorkflowType, j.ConfigFamilyID,
				strings.Join(j.FeedbackTypes, ",")})
		}
		return table(rows)
	case []projection.ConfigFamily:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"FAMILY", "APPLICATION", "SCHEMA", "HOLDOUT", "WORKFLOW"})
		for _, f := range value {
			rows = append(rows, []string{f.Name, f.ApplicationID, f.SchemaRevision,
				boolMark(f.RequiresHoldout), f.WorkflowType})
		}
		return table(rows)
	case []projection.ManagedTarget:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"PATH", "TYPE", "RELOAD", "ATOMICITY", "FAMILY"})
		for _, t := range value {
			rows = append(rows, []string{t.Path, t.TargetType, t.ReloadPolicy, t.Atomicity, t.ConfigFamilyID})
		}
		return table(rows)
	case []projection.Event:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"TIME", "TYPE", "APPLICATION", "REASON", "CONFIG", "SEQ"})
		for _, e := range value {
			rows = append(rows, []string{stamp(e.ReceivedAt), e.FeedbackType, shortID(e.ApplicationID),
				e.ReasonCode, shortID(e.ConfigRevision), fmt.Sprint(e.LedgerSequence)})
		}
		return table(rows)
	case []projection.Issue:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"ISSUE", "TYPE", "REASON", "STATUS", "COUNT", "SRC", "PATTERN"})
		for _, i := range value {
			rows = append(rows, []string{shortID(i.IssueID), i.FeedbackType, i.ReasonCode,
				i.Status, fmt.Sprint(i.OccurrenceCount), fmt.Sprint(i.UniqueSources), truncate(i.Pattern, 44)})
		}
		return table(rows)
	case projection.Issue:
		return fmt.Sprintf("issue    %s\njunction %s\ntype     %s %s\nstatus   %s\ncount    %d from %d sources\npattern  %s\nfirst    %s\nlast     %s",
			value.IssueID, value.JunctionID, value.FeedbackType, value.ReasonCode, value.Status,
			value.OccurrenceCount, value.UniqueSources, value.Pattern,
			stamp(value.FirstSeen), stamp(value.LastSeen))
	case []projection.WorkItem:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"WORK", "WORKFLOW", "STATE", "ISSUES", "ATTEMPTS", "LEASE"})
		for _, w := range value {
			lease := w.LeaseOwner
			if !w.LeaseExpiresAt.IsZero() {
				lease = fmt.Sprintf("%s until %s", w.LeaseOwner, w.LeaseExpiresAt.Format(time.RFC3339))
			}
			rows = append(rows, []string{shortID(w.WorkID), w.WorkflowType, w.State,
				fmt.Sprint(len(w.IssueIDs)), fmt.Sprint(w.Attempts), lease})
		}
		return table(rows)
	case projection.WorkItem:
		return fmt.Sprintf("work      %s\nworkflow  %s\nstate     %s\nissues    %s\nattempts  %d\nlease     %s %s\npackage   %s",
			value.WorkID, value.WorkflowType, value.State, strings.Join(value.IssueIDs, ", "),
			value.Attempts, value.LeaseOwner, leaseExpiry(value), compact(value.Package))
	case []projection.ConfigRevision:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"REV", "REVISION_ID", "LABEL", "MESSAGE", "TREE", "CREATED"})
		for _, r := range value {
			rows = append(rows, []string{fmt.Sprintf("r%d", r.Sequence), r.RevisionID, r.Label,
				truncate(r.Message, 36), shortHash(r.RootTreeHash), stamp(r.CreatedAt)})
		}
		return table(rows)
	case projection.ConfigRevision:
		return fmt.Sprintf("revision r%d  %s\nlabel    %s\nschema   %s\ntree     %s\ncontent  %s\nparent   %s\nmessage  %s\ncreated  %s",
			value.Sequence, value.RevisionID, value.Label, value.SchemaRevision,
			value.RootTreeHash, value.ContentHash, value.ParentRevisionID, value.Message, stamp(value.CreatedAt))
	case []projection.Candidate:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"CANDIDATE", "STATE", "BASE", "TREE", "EXPLANATION"})
		for _, c := range value {
			rows = append(rows, []string{c.CandidateID, c.State, shortID(c.BaseRevisionID),
				shortHash(c.RootTreeHash), truncate(c.Explanation, 40)})
		}
		return table(rows)
	case projection.Candidate:
		return fmt.Sprintf("candidate %s\nstate     %s\nbase      %s\ntree      %s\nwork      %s\nexplained %s",
			value.CandidateID, value.State, value.BaseRevisionID, value.RootTreeHash,
			value.WorkItemID, value.Explanation)
	case []projection.ConfigRef:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"REF", "REVISION", "CONTENT", "UPDATED"})
		for _, r := range value {
			rows = append(rows, []string{r.Name, shortID(r.RevisionID), shortHash(r.ContentHash), stamp(r.UpdatedAt)})
		}
		return table(rows)
	case []projection.ReflogEntry:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"TIME", "REF", "FROM", "TO", "REASON"})
		for _, e := range value {
			rows = append(rows, []string{stamp(e.CreatedAt), e.RefName, shortID(e.OldRevisionID),
				shortID(e.NewRevisionID), truncate(e.Reason, 40)})
		}
		return table(rows)
	case []projection.AuditEvent:
		rows := make([][]string, 0, len(value)+1)
		rows = append(rows, []string{"TIME", "ACTION", "ACTOR", "SUBJECT"})
		for _, e := range value {
			rows = append(rows, []string{stamp(e.CreatedAt), e.Action, e.Actor, shortID(e.SubjectID)})
		}
		return table(rows)
	case map[string]any:
		return jsonBlock(value)
	default:
		raw, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(raw)
	}
}

func statusBlock(s engine.Status) string {
	var b strings.Builder
	fmt.Fprintf(&b, "instance      %s\n", s.InstanceUUID)
	fmt.Fprintf(&b, "root          %s\n", s.Root)
	fmt.Fprintf(&b, "ledger        %d records, sequence %d, head %s\n",
		s.LedgerRecords, s.LedgerSequence, shortHash(s.LedgerHead))
	fmt.Fprintf(&b, "projection    %d records projected", s.ProjectedRecords)
	if s.DriftRecords > 0 {
		fmt.Fprintf(&b, " (%d pending)", s.DriftRecords)
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "applications  %d\n", s.Applications)
	fmt.Fprintf(&b, "junctions     %d\n", s.Junctions)
	fmt.Fprintf(&b, "events        %d\n", s.Events)
	fmt.Fprintf(&b, "objects       %d (%s)\n", s.ObjectCount, humanBytes(s.ObjectBytes))
	b.WriteString("issues\n")
	if len(s.Issues) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, status := range sortedKeys(s.Issues) {
		fmt.Fprintf(&b, "  %-24s %d\n", status, s.Issues[status])
	}
	b.WriteString("work items\n")
	if len(s.WorkItems) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, state := range sortedKeys(s.WorkItems) {
		fmt.Fprintf(&b, "  %-24s %d\n", state, s.WorkItems[state])
	}
	fmt.Fprintf(&b, "candidates    %d\n", s.Candidates)
	if s.ServerVersion != "" || len(s.ProtocolVersions) > 0 {
		fmt.Fprintf(&b, "server        %s (protocol %s)\n", s.ServerVersion, strings.Join(s.ProtocolVersions, ", "))
	}
	if len(s.Sessions) > 0 || s.HandshakesTotal > 0 {
		b.WriteString("sessions\n")
		for _, state := range sortedKeys(s.Sessions) {
			fmt.Fprintf(&b, "  %-24s %d\n", state, s.Sessions[state])
		}
		fmt.Fprintf(&b, "handshakes    %d total, %d rejected, %d reconnects\n",
			s.HandshakesTotal, s.HandshakeRejections, s.SessionReconnects)
		fmt.Fprintf(&b, "traffic       %d sessionless events, %d spool replays\n",
			s.SessionlessEvents, s.SpoolReplays)
	}
	return strings.TrimRight(b.String(), "\n")
}

func jsonBlock(v any) string {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(raw)
}

func table(rows [][]string) string {
	if len(rows) == 0 {
		return "(none)"
	}
	widths := make([]int, len(rows[0]))
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	var b strings.Builder
	for _, row := range rows {
		for i, cell := range row {
			if i == len(row)-1 {
				b.WriteString(cell)
				break
			}
			fmt.Fprintf(&b, "%-*s  ", widths[i], cell)
		}
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func shortID(id string) string {
	if id == "" {
		return "-"
	}
	if len(id) <= 12 {
		return id
	}
	// UUIDv7 leads with a timestamp, so the first bytes of two identities look
	// alike. The tail is what distinguishes them on screen.
	return "…" + id[len(id)-8:]
}

func shortHash(h string) string {
	if h == "" {
		return "-"
	}
	h = strings.TrimPrefix(h, "sha256:")
	if len(h) <= 16 {
		return h
	}
	return h[:12] + "…"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

func compact(s string) string {
	if s == "" {
		return "-"
	}
	return truncate(strings.ReplaceAll(s, "\n", " "), 72)
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func boolMark(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func newTag(created bool) string {
	if created {
		return "  (new)"
	}
	return ""
}

func changedTag(changed bool) string {
	if changed {
		return " (new revision)"
	}
	return " (unchanged manifest)"
}

func leaseExpiry(w projection.WorkItem) string {
	if w.LeaseExpiresAt.IsZero() {
		return ""
	}
	return "until " + w.LeaseExpiresAt.Local().Format(time.RFC3339)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
