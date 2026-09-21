#!/usr/bin/env python3
"""Export an application's accumulated feedback for later development.

This is the retrieval half of the integration: months after the fact, you want
to know which junctions kept failing, which phrases nobody taught the
classifier, and which of those an improvement actually silenced. The daemon is
the write path; this is a read-only reader over the store it produced.

It reads the SQLite projection, not the ledger, because the projection is the
shape that answers questions (events joined to the issue they were grouped
into). The projection is derived, so this script is also a good way to notice
that it is stale: `lymphctl rebuild` regenerates it from the ledger, and the
event and issue rows come back identical.

    python3 scripts/export-feedback.py --application semantic-service --out ./semantic-service-feedback
    python3 scripts/export-feedback.py --application semantic-service --since 2026-08-01 \
        --out ./semantic-service-feedback

Output (all UTF-8, all deterministic, safe to diff and commit):

    summary.json         counts, date range, and histograms that answer
                         "where is the pain concentrated?"
    issues.ndjson        one issue per line, highest occurrence first
    events.ndjson        one event per line, each carrying its issue_id
    occurrences.ndjson   the explicit event-to-issue link, with timing

Read-only by construction: the connection is opened in SQLite's query_only
mode, so a bug in this script cannot corrupt the store it is reading.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_ROOT = "/var/lib/lymph"


def find_store(root: Path) -> tuple[Path, Path]:
    """Locate the projection database inside a Lymph store."""
    db_dir = root / "db"
    if not db_dir.is_dir():
        raise SystemExit(
            f"{root} does not look like a Lymph store: no db/ directory. "
            f"Pass --root, or set LYMPH_ROOT."
        )
    candidates = sorted(db_dir.glob("*.sqlite"))
    if not candidates:
        raise SystemExit(f"no SQLite projection under {db_dir}")
    return root, candidates[0]


def resolve_application(connection: sqlite3.Connection, reference: str) -> dict[str, str]:
    """Accept an application UUID or its registered name."""
    row = connection.execute(
        "SELECT application_id, name FROM applications WHERE application_id = ? OR name = ?",
        (reference, reference),
    ).fetchone()
    if row is None:
        known = [r[0] for r in connection.execute("SELECT name FROM applications ORDER BY name")]
        raise SystemExit(
            f"no application {reference!r} in this store. Known: {', '.join(known) or 'none'}"
        )
    return {"application_id": row[0], "name": row[1]}


def open_readonly(path: Path) -> sqlite3.Connection:
    """Open the projection without any possibility of writing to it.

    query_only is used rather than mode=ro on purpose: a write-ahead-log
    database needs to create its shared-memory file even for a reader, and a
    strict read-only open fails when the daemon holds it. query_only gives the
    same guarantee about writes without that failure mode.
    """
    connection = sqlite3.connect(f"file:{path}", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = 1")
    return connection


def fetch_issues(connection: sqlite3.Connection, application_id: str, since: str) -> list[dict]:
    query = """
        SELECT i.issue_id, i.junction_id, j.name AS junction_name,
               i.feedback_type, i.reason_code, i.pattern, i.status,
               i.severity, i.priority, i.occurrence_count, i.unique_sources,
               i.controlling_revision, i.first_seen, i.last_seen
        FROM issues i
        LEFT JOIN junctions j ON j.junction_id = i.junction_id
        WHERE i.application_id = ?
    """
    parameters: list[object] = [application_id]
    if since:
        query += " AND i.last_seen >= ?"
        parameters.append(since)
    query += " ORDER BY i.occurrence_count DESC, i.last_seen DESC"

    issues = []
    for row in connection.execute(query, parameters):
        issue = dict(row)
        # Names are metadata; the UUIDs remain the identity. Both are exported so
        # a human can read the file and a machine can join it.
        issue["junction"] = issue.pop("junction_name", "") or issue["junction_id"]
        issues.append(issue)
    return issues


def fetch_events(connection: sqlite3.Connection, application_id: str,
                 since: str, limit: int) -> list[dict]:
    query = """
        SELECT e.event_id, e.junction_id, j.name AS junction_name,
               e.installation_id, e.feedback_type, e.reason_code, e.fingerprint,
               e.producer_instance, e.session_id, e.replayed_by_session_id,
               e.config_revision, e.config_hash, e.input_ref, e.replay_ref,
               e.durability, e.data, e.occurred_at, e.received_at, e.ledger_sequence,
               o.issue_id
        FROM events e
        LEFT JOIN issue_occurrences o ON o.event_id = e.event_id
        LEFT JOIN junctions j ON j.junction_id = e.junction_id
        WHERE e.application_id = ?
    """
    parameters: list[object] = [application_id]
    if since:
        query += " AND e.received_at >= ?"
        parameters.append(since)
    query += " ORDER BY e.received_at DESC, e.event_id"
    if limit:
        query += " LIMIT ?"
        parameters.append(limit)

    events: list[dict] = []
    for row in connection.execute(query, parameters):
        event = dict(row)
        # `data` is stored as the JSON string that arrived on the wire. It is
        # decoded here so a consumer does not have to know that.
        raw = event.pop("data", "") or ""
        try:
            event["data"] = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            # Never silently drop what an application actually sent: keep the
            # bytes and say they did not parse.
            event["data"] = {"unparsed": raw}
        event["junction"] = event.pop("junction_name", "") or event.get("junction_id", "")
        events.append(event)
    return events


def fetch_occurrences(connection: sqlite3.Connection, application_id: str,
                      since: str) -> list[dict]:
    query = """
        SELECT o.issue_id, o.event_id, o.seen_at, o.producer,
               i.reason_code, i.feedback_type, i.junction_id
        FROM issue_occurrences o
        JOIN issues i ON i.issue_id = o.issue_id
        WHERE i.application_id = ?
    """
    parameters: list[object] = [application_id]
    if since:
        query += " AND o.seen_at >= ?"
        parameters.append(since)
    query += " ORDER BY o.seen_at, o.event_id"
    return [dict(row) for row in connection.execute(query, parameters)]


def name_map(connection: sqlite3.Connection, table: str, id_column: str) -> dict[str, str]:
    """Read an id-to-name map for the histograms, so the summary is readable."""
    try:
        rows = connection.execute(f"SELECT {id_column}, name FROM {table}").fetchall()
    except sqlite3.Error:
        return {}
    return {row[0]: (row[1] or row[0]) for row in rows}


def histogram(values: list[str]) -> dict[str, int]:
    return dict(sorted(Counter(values).items(), key=lambda item: (-item[1], item[0])))


def write_ndjson(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", default=os.environ.get("LYMPH_ROOT", DEFAULT_ROOT),
                        help="Lymph store directory")
    parser.add_argument("--application", required=True,
                        help="application UUID or registered name")
    parser.add_argument("--out", required=True, help="directory to write the export into")
    parser.add_argument("--since", default="",
                        help="only events/issues seen at or after this RFC3339 or YYYY-MM-DD stamp")
    parser.add_argument("--limit", type=int, default=0, help="cap the number of events exported")
    args = parser.parse_args(argv)

    since = args.since
    if since and len(since) == 10:  # a bare date means "from the start of that day"
        since = since + "T00:00:00Z"

    root, database = find_store(Path(args.root))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    connection = open_readonly(database)
    try:
        application = resolve_application(connection, args.application)
        app_id = application["application_id"]

        issues = fetch_issues(connection, app_id, since)
        events = fetch_events(connection, app_id, since, args.limit)
        occurrences = fetch_occurrences(connection, app_id, since)
        junctions = name_map(connection, "junctions", "junction_id")
        installations = name_map(connection, "installations", "installation_id")
    finally:
        connection.close()

    write_ndjson(out / "issues.ndjson", issues)
    write_ndjson(out / "events.ndjson", events)
    write_ndjson(out / "occurrences.ndjson", occurrences)

    times = sorted(e["received_at"] for e in events if e["received_at"])
    summary = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "store": str(root),
        "projection": str(database),
        "read_only": True,
        "application": application,
        "since": since,
        "counts": {
            "events": len(events),
            "issues": len(issues),
            "occurrences": len(occurrences),
            "open_issues": sum(1 for i in issues if i["status"] == "OPEN"),
        },
        "window": {"first_event": times[0] if times else "", "last_event": times[-1] if times else ""},
        # The three questions worth asking of an accumulated history.
        # by_junction counts issues; occurrences_by_junction counts the pain
        # behind them. The two answer different questions on purpose.
        "by_junction": histogram([junctions.get(i["junction_id"], i["junction_id"]) for i in issues]),
        "occurrences_by_junction": histogram([junctions.get(o["junction_id"], o["junction_id"])
                                              for o in occurrences]),
        "by_reason_code": histogram([i["reason_code"] for i in issues if i["reason_code"]]),
        "by_feedback_type": histogram([i["feedback_type"] for i in issues]),
        "occurrences_by_reason_code": histogram([o["reason_code"] for o in occurrences if o["reason_code"]]),
        "events_by_installation": histogram([installations.get(e["installation_id"], e["installation_id"])
                                             for e in events]),
        "events_replayed_from_spool": sum(1 for e in events if e["replayed_by_session_id"]),
        "events_without_an_issue": sum(1 for e in events if not e["issue_id"]),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n",
                                      encoding="utf-8")

    print(f"exported {summary['counts']['events']} event(s) and "
          f"{summary['counts']['issues']} issue(s) from {application['name']}")
    print(f"window: {summary['window']['first_event'] or '-'} .. {summary['window']['last_event'] or '-'}")
    # Both numbers, because they answer different questions: occurrences are the
    # pain, issues are the number of distinct things to fix.
    print(f"{'occurrences':>12}  {'issues':>6}  reason_code")
    issues_by_reason = summary["by_reason_code"]
    for reason, occurrences in list(summary["occurrences_by_reason_code"].items())[:5]:
        print(f"{occurrences:>12}  {issues_by_reason.get(reason, 0):>6}  {reason}")
    print(f"written to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
