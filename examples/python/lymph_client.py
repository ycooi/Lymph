"""Minimal Lymph client for Python applications (design section 62).

Standard library only, no dependencies, because the applications that emit
feedback must not gain a package manager problem for doing so.

The contract that matters (design section 66):

    An application must never wait for Lymph.

So `emit()` tries the socket first; if the daemon is unreachable it appends the
event to a local spool and returns success. The event carries its id before it
is ever sent, so a later resend is deduplicated by the daemon
(`design section 67, section 68`).

Usage:

    from lymph_client import LymphClient, feedback

    lymph = LymphClient(
        socket_path="/run/lymph/lymph.sock",
        spool_dir=".lymph-spool",
        producer_instance="semantic-service-1",
        installation_id="digitalocean",
    )

    lymph.emit(
        application="019a8a51-...",           # the application UUID from registration
        junction="semantic.event_state",      # junction UUID or name
        feedback_type=feedback.UNKNOWN,
        reason_code="UNKNOWN_EVENT_PHRASE",
        payload={"text": "Acme business was booked at 512.00"},
        input_ref="email://2026-09-20/8842",
        config_revision="019abc...",          # what production was running
    )

    # Later, opportunistically:
    lymph.flush_spool()

Command line, for operators:

    python lymph_client.py --socket /run/lymph/lymph.sock flush
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import json
import os
import socket
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


SPEC_VERSION = "1.0"
# Wire protocol version. It is independent of the daemon's release version: the
# daemon may upgrade without changing this number.
PROTOCOL_VERSION = "1"
SESSION_HEADER = "X-Lymph-Session"
REPLAY_HEADER = "X-Lymph-Replay"
DEFAULT_SOCKET = "/run/lymph/lymph.sock"
DEFAULT_SPOOL_LIMIT = 10000


class feedback:
    """The universal feedback taxonomy (design section 9)."""

    UNKNOWN = "UNKNOWN"
    AMBIGUOUS = "AMBIGUOUS"
    CONFLICT = "CONFLICT"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    OUT_OF_CONTRACT = "OUT_OF_CONTRACT"
    QUALITY_FAILURE = "QUALITY_FAILURE"
    REGRESSION = "REGRESSION"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    PERFORMANCE_DRIFT = "PERFORMANCE_DRIFT"
    HUMAN_CORRECTION = "HUMAN_CORRECTION"
    RUNTIME_FAILURE = "RUNTIME_FAILURE"

    @classmethod
    def all(cls) -> tuple[str, ...]:
        return (
            cls.UNKNOWN, cls.AMBIGUOUS, cls.CONFLICT, cls.LOW_CONFIDENCE,
            cls.OUT_OF_CONTRACT, cls.QUALITY_FAILURE, cls.REGRESSION,
            cls.SCHEMA_DRIFT, cls.PERFORMANCE_DRIFT, cls.HUMAN_CORRECTION,
            cls.RUNTIME_FAILURE,
        )

    @classmethod
    def event_type(cls, value: str) -> str:
        if value not in cls.all():
            raise ValueError(f"unknown feedback type {value!r}")
        return f"lymph.feedback.{value.lower()}.v1"


def new_event_id() -> str:
    """UUIDv7 where available, UUID4 otherwise.

    Identity is the event's, not the daemon's: the id exists before the first
    delivery attempt so that retries are idempotent.
    """
    try:
        from uuid import uuid7  # Python 3.14+

        return str(uuid7())
    except Exception:
        # RFC 9562 layout built by hand: 48-bit milliseconds, version 7, random.
        millis = int(time.time() * 1000)
        rand = uuid.uuid4().int
        value = (millis & 0xFFFFFFFFFFFF) << 80
        value |= 0x7 << 76
        value |= (rand >> 64) & 0xFFF << 64
        value |= 0b10 << 62
        value |= rand & ((1 << 62) - 1)
        return str(uuid.UUID(int=value))


class LymphError(RuntimeError):
    """Raised when the daemon answers with an error."""


class IntegrityError(LymphError):
    """The delivered bytes do not hash to what the approval covers.

    The node must not apply content that fails this check: the point of the
    content hash is that the node verifies it rather than trusting the daemon
    (mission section 29).
    """


class HandshakeRejectedError(LymphError):
    """The daemon is reachable and refused this identity.

    Not an outage: spooling would hide a configuration mistake, so this is
    raised (mission section 28).
    """

    def __init__(self, reason_code: str, message: str = "") -> None:
        super().__init__(f"lymph refused this identity: {reason_code}: {message}")
        self.reason_code = reason_code
        self.message = message


class _UnixHTTPConnection(http.client.HTTPConnection):
    """HTTP over a Unix domain socket (design section 63).

    HTTP+JSON was chosen over a bespoke binary protocol so that curl, Go and
    Python are all first-class clients of the same interface.
    """

    def __init__(self, socket_path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self.socket_path = socket_path

    def connect(self) -> None:  # type: ignore[override]
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.socket_path)
        self.sock = sock


@dataclass
class EmitResult:
    event_id: str
    accepted: bool
    spooled: bool = False
    duplicate: bool = False
    issue_id: str = ""
    issue_created: bool = False
    occurrence_count: int = 0
    ledger_sequence: int = 0
    fingerprint: str = ""

    # Mapping-style access, so both styles read naturally:
    #
    #   result.issue_id
    #   result["issue_id"]
    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def keys(self):
        return self.__dataclass_fields__.keys()

    @classmethod
    def from_response(cls, body: Mapping[str, Any]) -> "EmitResult":
        return cls(
            event_id=body.get("event_id", ""),
            accepted=bool(body.get("accepted")),
            duplicate=bool(body.get("duplicate")),
            issue_id=body.get("issue_id", ""),
            issue_created=bool(body.get("issue_created")),
            occurrence_count=int(body.get("occurrence_count", 0)),
            ledger_sequence=int(body.get("ledger_sequence", 0)),
            fingerprint=body.get("fingerprint", ""),
        )


@dataclass
class Spool:
    """A bounded local queue of undelivered events (design section 67).

    Bounded on purpose: an application must never grow an unbounded queue
    because the daemon stayed down for a month.
    """

    directory: str
    limit: int = DEFAULT_SPOOL_LIMIT
    _path: str = field(init=False)

    def __post_init__(self) -> None:
        self._path = os.path.join(self.directory, "incoming.jsonl")

    def append(self, envelope: Mapping[str, Any]) -> None:
        os.makedirs(self.directory, mode=0o700, exist_ok=True)
        if self.count() >= self.limit:
            raise LymphError(
                f"lymph spool is full ({self.limit} events); refusing to grow without bound"
            )
        line = json.dumps(envelope, separators=(",", ":")) + "\n"
        with open(self._path, "a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())

    def pending(self) -> list[dict[str, Any]]:
        if not os.path.exists(self._path):
            return []
        entries: list[dict[str, Any]] = []
        with open(self._path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
        return entries

    def count(self) -> int:
        return len(self.pending())

    def clear(self) -> None:
        try:
            os.remove(self._path)
        except FileNotFoundError:
            pass

    def drop_prefix(self, n: int) -> None:
        """Remove the first n delivered entries, keeping the rest.

        A flush that fails halfway keeps what was not delivered, so a long
        outage does not resend everything (mission section 34).
        """
        if n <= 0:
            return
        pending = self.pending()
        if n >= len(pending):
            self.clear()
            return
        remaining = pending[n:]
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            for entry in remaining:
                handle.write(json.dumps(entry, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self._path)


class LymphClient:
    def __init__(
        self,
        socket_path: str = DEFAULT_SOCKET,
        spool_dir: str | None = None,
        producer_instance: str | None = None,
        installation_id: str | None = None,
        application_id: str | None = None,
        process_id: str | None = None,
        client_name: str | None = None,
        client_version: str | None = None,
        manifest_hash: str | None = None,
        auto_handshake: bool = True,
        timeout: float = 5.0,
        spool_limit: int = DEFAULT_SPOOL_LIMIT,
    ) -> None:
        self.socket_path = socket_path
        # Process and producer instance are the same concept; whichever is
        # given wins, and one is generated once per client otherwise.
        self.producer_instance = producer_instance or process_id or new_event_id()
        self.process_id = self.producer_instance
        self.application_id = application_id
        self.installation_id = installation_id
        self.client_name = client_name or "lymph-python"
        self.client_version = client_version or "0.2.0"
        self.manifest_hash = manifest_hash
        self.auto_handshake = auto_handshake
        self.session: dict[str, Any] | None = None
        # The last handshake the daemon refused, if any. Empty means "no
        # rejection seen"; an application can check it to tell a bad identity
        # apart from an unreachable daemon.
        self.last_handshake_error = ""
        self.timeout = timeout
        self.spool = Spool(spool_dir, spool_limit) if spool_dir else None

    @classmethod
    def from_identity_file(cls, path: str, spool_dir: str | None = None, **kwargs: Any) -> "LymphClient":
        """Build a client from an application identity file.

        The file is written by registration tooling, not by this client:

            {"application_id": "...", "installation_id": "..."}
        """
        with open(path, encoding="utf-8") as handle:
            identity = json.load(handle)
        application_id = identity.get("application_id", "")
        if not application_id:
            raise LymphError(f"{path} has no application_id")
        return cls(
            socket_path=identity.get("socket", kwargs.pop("socket_path", DEFAULT_SOCKET)),
            spool_dir=spool_dir,
            application_id=application_id,
            installation_id=identity.get("installation_id"),
            **kwargs,
        )

    # ---------- handshake ----------

    def hello(self) -> dict[str, Any]:
        """Perform the handshake and keep the session.

        Applications rarely call this: emit() does it lazily.
        """
        if not self.application_id or not self.installation_id:
            raise LymphError("handshake needs application_id and installation_id")
        request = {
            "protocol_version": PROTOCOL_VERSION,
            "application_id": self.application_id,
            "installation_id": self.installation_id,
            "process_id": self.process_id,
            "client_name": self.client_name,
            "client_version": self.client_version,
        }
        if self.manifest_hash:
            request["manifest_hash"] = self.manifest_hash
        try:
            request["pid"] = os.getpid()
        except OSError:
            pass
        try:
            request["hostname"] = socket.gethostname()
        except OSError:
            pass

        response = self._request("POST", "/v1/hello", request)
        if not response.get("accepted"):
            raise HandshakeRejectedError(
                response.get("reason_code", "UNKNOWN"),
                response.get("message", ""),
            )
        self.session = response
        return response

    def _ensure_session(self) -> None:
        if self.session is not None or not self.auto_handshake:
            return
        if not self.application_id or not self.installation_id:
            return
        try:
            self.hello()
        except LymphError as error:
            # The daemon answered and refused this identity: that is a
            # configuration mistake, not an outage, and the sessionless fallback
            # would otherwise hide it forever, because events keep flowing
            # without a session and nothing looks wrong. It is recorded so an
            # application can ask why it has no session (mission section 62).
            self.last_handshake_error = str(error)
            raise

    def session_info(self) -> dict[str, Any]:
        if self.session is None:
            raise LymphError("no session; call hello() first or enable auto_handshake")
        return self.session

    # ---------- transport ----------

    def _request(self, method: str, path: str, body: Any | None = None,
                 headers: Mapping[str, str] | None = None) -> Any:
        payload = None
        request_headers: dict[str, str] = dict(headers or {})
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
            request_headers["Content-Length"] = str(len(payload))

        conn = _UnixHTTPConnection(self.socket_path, self.timeout)
        try:
            conn.request(method, path, body=payload, headers=request_headers)
            response = conn.getresponse()
            raw = response.read()
        finally:
            conn.close()

        if response.status == 204:
            return None
        if response.status >= 400:
            try:
                detail = json.loads(raw.decode("utf-8")).get("error", raw.decode("utf-8"))
            except Exception:
                detail = raw.decode("utf-8", "replace")
            raise LymphError(f"{detail} (status {response.status})")
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))

    # ---------- the one call an application actually needs ----------

    def build_event(
        self,
        application: str,
        junction: str,
        feedback_type: str,
        reason_code: str = "",
        payload: Any | None = None,
        input_ref: str = "",
        replay_ref: str = "",
        config_revision: str = "",
        config_hash: str = "",
        contract_revision: str = "",
        event_id: str | None = None,
        subject: str | None = None,
        time_utc: str | None = None,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {"feedback_type": feedback_type}
        if reason_code:
            data["reason_code"] = reason_code
        if payload is not None:
            data["payload"] = payload
        for key, value in (
            ("input_ref", input_ref),
            ("replay_ref", replay_ref),
            ("config_revision", config_revision),
            ("config_hash", config_hash),
            ("contract_revision", contract_revision),
        ):
            if value:
                data[key] = value

        return {
            "specversion": SPEC_VERSION,
            "id": event_id or new_event_id(),
            "source": f"lymph://{application}/{junction}",
            "type": feedback.event_type(feedback_type),
            "subject": subject or junction,
            "time": time_utc or _rfc3339_now(),
            "datacontenttype": "application/json",
            "data": data,
        }

    def emit(
        self,
        application: str = "",
        junction: str = "",
        feedback_type: str = "",
        reason_code: str = "",
        payload: Any | None = None,
        durability: str = "ASYNC",
        fingerprint: str | None = None,
        producer_instance: str | None = None,
        **kwargs: Any,
    ) -> EmitResult:
        """Submit feedback without ever blocking the application.

        Returns EmitResult; `spooled=True` means Lymph was unreachable and the
        event is queued locally instead.
        """
        application = application or self.application_id or ""
        if not application:
            raise LymphError("emit needs an application id: pass application= or construct with application_id=")
        event = self.build_event(
            application=application,
            junction=junction,
            feedback_type=feedback_type,
            reason_code=reason_code,
            payload=payload,
            **kwargs,
        )
        envelope = dict(event)
        envelope["producer_instance"] = producer_instance or self.producer_instance or ""
        envelope["installation_id"] = self.installation_id or ""
        envelope["durability"] = durability
        if fingerprint:
            envelope["fingerprint"] = fingerprint

        if not envelope["producer_instance"]:
            envelope.pop("producer_instance")
        if not envelope["installation_id"]:
            envelope.pop("installation_id")

        # A session makes the daemon's validation stricter and lets it fill in
        # installation and process. An unreachable daemon is not an error here:
        # the event goes to the spool either way.
        headers: dict[str, str] = {}
        try:
            self._ensure_session()
        except (OSError, LymphError, http.client.HTTPException):
            self.session = None
        if self.session is not None:
            headers[SESSION_HEADER] = self.session.get("session_id", "")

        try:
            body = self._request("POST", "/v1/events", envelope, headers)
            return EmitResult.from_response(body or {})
        except (OSError, LymphError, http.client.HTTPException):
            if self.spool is None:
                raise
            self.spool.append(envelope)
            return EmitResult(event_id=event["id"], accepted=True, spooled=True)

    def emit_feedback(self, **kwargs: Any) -> EmitResult:
        """Alias for emit() that reads naturally at a failure boundary.

        Typical use:

            result = classify(text)
            if result.unknown:
                lymph.emit_feedback(
                    junction="semantic.event_state",
                    feedback_type="UNKNOWN",
                    reason_code="NO_RULE_MATCH",
                    payload={"text": text},
                )
        """
        return self.emit(**kwargs)

    def flush_spool(self) -> int:
        """Resend everything spooled, oldest first.

        Events keep their original UUID and their original producer identity;
        the replay header tells the daemon that the producing process may differ
        from the session's. The daemon deduplicates, so a partial failure is safe
        to retry.
        """
        if self.spool is None:
            return 0
        try:
            self._ensure_session()
        except (OSError, LymphError, http.client.HTTPException):
            self.session = None
            return 0

        sent = 0
        headers = {}
        if self.session is not None:
            headers[SESSION_HEADER] = self.session.get("session_id", "")
        headers[REPLAY_HEADER] = "1"
        for envelope in self.spool.pending():
            self._request("POST", "/v1/events", envelope, headers)
            sent += 1
        self.spool.drop_prefix(sent)
        return sent

    # ---------- typed worker returns (L2.5) ----------

    def return_improvement(
        self,
        work_id: str,
        worker: str,
        attempt: int,
        artifacts: Iterable[Mapping[str, Any]],
        summary: str = "",
        workflow_run_id: str = "",
    ) -> dict[str, Any]:
        """Record one typed worker return.

        A worker says what it actually produced: a configuration bundle, a code
        reference, a model reference, a data fix, or the conclusion that nothing
        should change. Lymph canonicalises all of it in one atomic record, so a
        crash can never leave a candidate without its result.

        `attempt` is the lease token: pass the attempt number the work item gave
        you when you claimed it. A stale attempt is refused.
        """
        body: dict[str, Any] = {
            "worker": worker,
            "attempt": int(attempt),
            "summary": summary,
            "artifacts": list(artifacts),
        }
        if workflow_run_id:
            body["workflow_run_id"] = workflow_run_id
        return self._request("POST", f"/v1/work-items/{work_id}/return", body)

    def improvement_results(self, application: str = "", limit: int = 25) -> list[dict[str, Any]]:
        query = f"/v1/improvement-results?limit={limit}"
        if application:
            query += f"&application={application}"
        return (self._request("GET", query) or {}).get("improvement_results", [])

    def improvement_result(self, result_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/improvement-results/{result_id}") or {}

    def artifacts(self, kind: str = "", application: str = "", work_item: str = "") -> list[dict[str, Any]]:
        query = "/v1/artifacts"
        params = []
        if kind:
            params.append(f"kind={kind.upper()}")
        if application:
            params.append(f"application={application}")
        if work_item:
            params.append(f"work_item={work_item}")
        if params:
            query += "?" + "&".join(params)
        return (self._request("GET", query) or {}).get("artifacts", [])

    def installations(self, application: str = "") -> list[dict[str, Any]]:
        query = "/v1/installations"
        if application:
            query += f"?application={application}"
        return (self._request("GET", query) or {}).get("installations", [])

    # ---------- inspection helpers ----------

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/v1/health")

    def status(self) -> dict[str, Any]:
        return self._request("GET", "/v1/status")

    def issues(self, status: str = "", limit: int = 25, order: str = "count") -> list[dict[str, Any]]:
        query = f"/v1/issues?limit={limit}&order={order}"
        if status:
            query += f"&status={status}"
        return (self._request("GET", query) or {}).get("issues", [])

    def current_ref(self, application: str, config_family: str, name: str = "active") -> dict[str, Any]:
        """Ask what production is currently running.

        Useful for filling `config_revision` on the next event: every event
        should say which revision was in force when reality misbehaved.
        """
        refs = (self._request(
            "GET",
            f"/v1/refs?application={application}&config_family={config_family}",
        ) or {}).get("refs", [])
        for ref in refs:
            if ref.get("name") == name:
                return ref
        return {}

    # ---------- approved update delivery (L2.7) ----------
    #
    # The loop is a pull, so there is deliberately no apply_update() here.
    # Lymph says an exact revision is approved for this installation; the
    # application fetches the bytes, checks the hash itself, and only then does
    # whatever its own lifecycle requires. Only the application knows whether a
    # reload is safe.

    def _session_header(self) -> dict[str, str]:
        self._ensure_session()
        if self.session is None:
            raise LymphError("approved updates need a session; call hello() first")
        return {SESSION_HEADER: self.session["session_id"]}

    def check_updates(self, config_family: str = "") -> list[dict[str, Any]]:
        """Everything approved for this installation, newest first."""
        query = "/v1/updates"
        if config_family:
            query += f"?config_family={config_family}"
        body = self._request("GET", query, headers=self._session_header()) or {}
        return body.get("updates", [])

    def check_update(self, config_family: str, current_revision: str = "") -> dict[str, Any] | None:
        """The one call an update-capable application makes.

        Returns None when there is nothing to do, which is the normal answer and
        must stay quiet in the application's logs. Returns the offered update
        otherwise. If the offer was built on a different revision than the one
        passed in, the update is returned with base_compatible set to False: the
        caller needs to see what was offered to understand why it refuses.
        """
        updates = self.check_updates(config_family)
        if not updates:
            return None
        newest = updates[0]
        if current_revision and newest.get("revision_id") == current_revision:
            return None
        if not newest.get("base_compatible", True):
            newest = dict(newest)
            newest.setdefault("base_reason_code", "BASE_REVISION_MISMATCH")
            return newest
        # The daemon decides base compatibility, because it is the only party
        # that knows the disposition history: a revision the node refused, or an
        # operator withdrew, can never be applied, so a later revision built on
        # top of it is still reachable and the daemon lists it under
        # skipped_revisions. The client only checks the one case the daemon
        # cannot see: a caller that names a revision the daemon has no report
        # for at all.
        if (current_revision
                and newest.get("base_revision_id") not in ("", current_revision)
                and not newest.get("skipped_revisions")):
            newest = dict(newest)
            newest["base_compatible"] = False
            newest["base_reason_code"] = "BASE_REVISION_MISMATCH"
        return newest

    def fetch_update(self, config_family: str, revision_id: str) -> dict[str, Any]:
        """Fetch an approved revision and verify it locally.

        Raises IntegrityError if the delivered bytes do not hash to what the
        approval covers. The caller must not apply content that fails this.
        """
        import base64
        import hashlib

        path = f"/v1/updates/{revision_id}/content?config_family={config_family}"
        body = self._request("GET", path, headers=self._session_header()) or {}
        manifest = body.get("manifest", [])
        bundle = {name: base64.b64decode(data) for name, data in (body.get("bundle") or {}).items()}

        if not bundle or len(manifest) != len(bundle):
            raise IntegrityError(
                f"manifest lists {len(manifest)} files, bundle carries {len(bundle)}"
            )
        for entry in manifest:
            data = bundle.get(entry["path"])
            if data is None:
                raise IntegrityError(f"{entry['path']} is missing from the bundle")
            if len(data) != entry["size"]:
                raise IntegrityError(
                    f"{entry['path']} is {len(data)} bytes, manifest says {entry['size']}"
                )
            digest = "sha256:" + hashlib.sha256(data).hexdigest()
            if digest != entry["hash"]:
                raise IntegrityError(f"{entry['path']} hashes to {digest}, manifest says {entry['hash']}")

        recomputed = tree_hash(manifest)
        expected = body.get("root_tree_hash", "")
        if recomputed != expected:
            raise IntegrityError(f"bundle hashes to {recomputed}, approval covers {expected}")

        return {
            "update": body.get("update", {}),
            "root_tree_hash": expected,
            "manifest": manifest,
            "bundle": bundle,
        }

    def report_applied(self, revision_id: str, config_family: str,
                       observed_hash: str = "", previous_revision_id: str = "",
                       details: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Tell Lymph this installation is now running the revision."""
        body: dict[str, Any] = {"config_family_id": config_family}
        if observed_hash:
            body["observed_hash"] = observed_hash
        if previous_revision_id:
            body["previous_revision_id"] = previous_revision_id
        if details:
            body["details"] = dict(details)
        path = f"/v1/updates/{revision_id}/applied"
        return self._request("POST", path, body, headers=self._session_header()) or {}

    def report_rejected(self, revision_id: str, config_family: str, reason_code: str,
                        message: str = "", details: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Tell Lymph this installation will not run the revision.

        The reason code is a closed vocabulary: "it did not work" is not
        something a learning loop can act on.
        """
        if reason_code not in REJECTION_REASON_CODES:
            raise LymphError(f"reason_code {reason_code!r} is not one of {REJECTION_REASON_CODES}")
        body: dict[str, Any] = {"config_family_id": config_family, "reason_code": reason_code}
        if message:
            body["message"] = message
        if details:
            body["details"] = dict(details)
        path = f"/v1/updates/{revision_id}/rejected"
        return self._request("POST", path, body, headers=self._session_header()) or {}


def _rfc3339_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(time.time()*1000)%1000:03d}Z"


# ---------- content addressing the node does itself (L2.7) ----------

SHA256_PREFIX = "sha256:"


def content_hash(data: bytes) -> str:
    """The content address of a blob, computed here rather than asked for."""
    return SHA256_PREFIX + hashlib.sha256(data).hexdigest()


def tree_hash(manifest: Iterable[Mapping[str, Any]]) -> str:
    """Recompute the address of a bundle tree from its manifest.

    This mirrors the daemon's tree encoding exactly. A node that could not
    reproduce this hash would be trusting the daemon rather than checking it,
    which is the opposite of the point of the content hash.
    """
    entries = [
        {
            "path": entry["path"],
            "hash": entry["hash"],
            "mode": entry.get("mode", "100644"),
            "size": entry["size"],
        }
        for entry in manifest
    ]
    entries.sort(key=lambda entry: entry["path"])
    encoded = json.dumps({"kind": "tree", "entries": entries}, separators=(",", ":"))
    return SHA256_PREFIX + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


REJECTION_REASON_CODES = (
    "LOCAL_VALIDATION_FAILED",
    "SCHEMA_UNSUPPORTED",
    "RELOAD_FAILED",
    "HEALTH_CHECK_FAILED",
    "DEPENDENCY_MISSING",
    "OPERATOR_CANCELLED",
    "OTHER",
)


# ---------- artifact constructors (L2.5) ----------
#
# Every kind carries exactly one payload and Lymph rejects anything else, so
# these helpers exist to keep a worker from guessing the shape.


def config_bundle(bundle: Mapping[str, Any], explanation: str = "",
                  base_revision_id: str = "") -> dict[str, Any]:
    """A proposed configuration. The only kind that creates a candidate.

    Bundle contents are exact bytes, so they travel base64-encoded: pass str
    (encoded as UTF-8) or bytes and this helper does the encoding. Lymph hashes
    the bytes it receives, never a canonicalised form, so what you send is what
    it versions.
    """
    encoded = {}
    for path, content in bundle.items():
        if isinstance(content, str):
            content = content.encode("utf-8")
        encoded[path] = base64.b64encode(bytes(content)).decode("ascii")
    return {
        "kind": "CONFIG_BUNDLE",
        "config_bundle": {
            "bundle": encoded,
            "explanation": explanation,
            "base_revision_id": base_revision_id,
        },
    }


def code_ref(repository: str, commit: str, tree_hash: str = "", branch: str = "",
             description: str = "") -> dict[str, Any]:
    """A code change. Lymph records the reference; it never clones the repo."""
    return {
        "kind": "CODE_REF",
        "code_ref": {
            "repository": repository, "commit": commit, "tree_hash": tree_hash,
            "branch": branch, "description": description,
        },
    }


def model_ref(uri: str, digest: str, model_type: str = "", version: str = "",
              description: str = "") -> dict[str, Any]:
    """A model artifact that lives outside Lymph. Only URI and digest travel."""
    return {
        "kind": "MODEL_REF",
        "model_ref": {
            "uri": uri, "digest": digest, "model_type": model_type,
            "version": version, "description": description,
        },
    }


def data_fix_ref(patch_id: str = "", uri: str = "", digest: str = "",
                 target: str = "", description: str = "") -> dict[str, Any]:
    """A correction to data. Lymph records and routes it; it never applies it."""
    return {
        "kind": "DATA_FIX_REF",
        "data_fix_ref": {
            "patch_id": patch_id, "uri": uri, "digest": digest,
            "target": target, "description": description,
        },
    }


def no_change(reason: str, disposition: str = "") -> dict[str, Any]:
    """The conclusion that the current configuration is correct.

    Dispositions: CURRENT_CONFIG_CORRECT, TRANSIENT_UPSTREAM_FAILURE,
    FALSE_POSITIVE, DUPLICATE, EXTERNAL_CAUSE, OTHER.
    """
    return {"kind": "NO_CHANGE", "no_change": {"reason": reason, "disposition": disposition}}


def _main(argv: Iterable[str]) -> int:
    parser = argparse.ArgumentParser(description="Lymph client utilities")
    parser.add_argument("--socket", default=os.environ.get("LYMPH_SOCKET", DEFAULT_SOCKET))
    parser.add_argument("--spool", default=os.environ.get("LYMPH_SPOOL", ""))
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="daemon summary")
    sub.add_parser("health", help="liveness")
    sub.add_parser("flush", help="resend everything in the spool")

    emit = sub.add_parser("emit", help="submit one feedback event")
    emit.add_argument("--app", required=True)
    emit.add_argument("--junction", required=True)
    emit.add_argument("--type", required=True, choices=list(feedback.all()))
    emit.add_argument("--reason", default="")
    emit.add_argument("--payload", default="")
    emit.add_argument("--config-revision", default="")
    emit.add_argument("--producer", default="")
    emit.add_argument("--durability", default="ASYNC", choices=["ASYNC", "DURABLE"])

    args = parser.parse_args(list(argv))
    client = LymphClient(
        socket_path=args.socket,
        spool_dir=args.spool or None,
        producer_instance=os.environ.get("HOSTNAME", "") or None,
    )

    if args.command == "status":
        print(json.dumps(client.status(), indent=2))
    elif args.command == "health":
        print(json.dumps(client.health(), indent=2))
    elif args.command == "flush":
        print(f"delivered {client.flush_spool()} events")
    elif args.command == "emit":
        payload = json.loads(args.payload) if args.payload else None
        result = client.emit(
            application=args.app,
            junction=args.junction,
            feedback_type=args.type,
            reason_code=args.reason,
            payload=payload,
            durability=args.durability,
            producer_instance=args.producer or None,
            config_revision=args.config_revision,
        )
        print(json.dumps(result.__dict__, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
