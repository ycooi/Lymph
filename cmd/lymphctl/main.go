// Command lymphctl is the operator interface to a running Lymph daemon
// (section 62, section 96).
//
// Everything it does goes through the same Unix socket API the SDK uses, so
// anything lymphctl can do, an application or a worker can do too.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ycooi/Lymph/internal/buildinfo"
	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/pkg/client"
)

type ctl struct {
	socket  string
	root    string
	spool   string
	jsonOut bool
	client  *client.Client
}

func main() {
	args, g, err := peelGlobals(os.Args[1:])
	if err != nil {
		fail(err)
	}
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	c := &ctl{socket: g.socket, root: g.root, spool: g.spool, jsonOut: g.jsonOut}
	if c.socket == "" {
		c.socket = envOr("LYMPH_SOCKET", "")
	}
	if c.root == "" {
		c.root = envOr("LYMPH_ROOT", engine.DefaultRoot)
	}
	if c.socket == "" {
		c.socket = filepath.Join(c.root, "lymph.sock")
	}

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "help", "-h", "--help":
		usage()
		return
	case "version":
		fmt.Printf("lymphctl %s (built %s)\n", buildinfo.String(), buildinfo.Date)
		return
	case "init":
		c.cmdInit(rest)
		return
	}

	// Everything below talks to a running daemon.
	c.client = client.New(client.Options{
		Socket:           c.socket,
		SpoolDir:         c.spool,
		ProducerInstance: "lymphctl",
	})

	switch cmd {
	case "status":
		c.cmdStatus(rest)
	case "health":
		c.cmdHealth(rest)
	case "emit":
		c.cmdEmit(rest)
	case "events":
		c.cmdEvents(rest)
	case "register-app":
		c.cmdRegisterApp(rest)
	case "apps":
		c.cmdApps(rest)
	case "junctions":
		c.cmdJunctions(rest)
	case "families":
		c.cmdFamilies(rest)
	case "targets":
		c.cmdTargets(rest)
	case "issues":
		c.cmdIssues(rest)
	case "issue-status":
		c.cmdIssueStatus(rest)
	case "queue":
		c.cmdQueue(rest)
	case "work":
		c.cmdWork(rest)
	case "claim":
		c.cmdClaim(rest)
	case "complete":
		c.cmdComplete(rest)
	case "baseline":
		c.cmdBaseline(rest)
	case "revision":
		c.cmdRevision(rest)
	case "revisions":
		c.cmdRevisions(rest)
	case "lineage":
		c.cmdLineage(rest)
	case "diff":
		c.cmdDiff(rest)
	case "candidate":
		c.cmdCandidate(rest)
	case "candidates":
		c.cmdCandidates(rest)
	case "validate":
		c.cmdValidate(rest)
	case "approve":
		c.cmdApprove(rest)
	case "reject":
		c.cmdReject(rest)
	case "defer":
		c.cmdDefer(rest)
	case "review":
		c.cmdReview(rest)
	case "withdraw":
		c.cmdWithdraw(rest)
	case "updates":
		c.cmdUpdates(rest)
	case "application-results":
		c.cmdApplicationResults(rest)
	case "update-dispositions":
		c.cmdUpdateDispositions(rest)
	case "whoami":
		c.cmdWhoami(rest)
	case "promote":
		c.cmdPromote(rest)
	case "refs":
		c.cmdRefs(rest)
	case "installations":
		c.cmdInstallations(rest)
	case "sessions":
		c.cmdSessions(rest)
	case "session":
		c.cmdSession(rest)
	case "hello":
		c.cmdHello(rest)
	case "identity":
		c.cmdIdentity(rest)
	case "results":
		c.cmdResults(rest)
	case "result":
		c.cmdResult(rest)
	case "artifacts":
		c.cmdArtifacts(rest)
	case "artifact":
		c.cmdArtifact(rest)
	case "return-result":
		c.cmdReturnResult(rest)
	case "ref":
		c.cmdRef(rest)
	case "reflog":
		c.cmdReflog(rest)
	case "audit":
		c.cmdAudit(rest)
	case "ledger":
		c.cmdLedger(rest)
	case "rebuild":
		c.cmdRebuild(rest)
	case "object":
		c.cmdObject(rest)
	case "deploy":
		fmt.Fprintln(os.Stderr, "deployment adapters are L3 and are not implemented in this build.")
		fmt.Fprintln(os.Stderr, "see docs/IMPLEMENTATION-STATUS.md: Lymph proves memory and versioning before it touches production files.")
		os.Exit(3)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

type globals struct {
	socket  string
	root    string
	spool   string
	jsonOut bool
}

// peelGlobals pulls --socket, --root, --spool and --json out of the argument
// list wherever they appear, so both `lymphctl --json status` and
// `lymphctl status --json` work.
func peelGlobals(args []string) ([]string, globals, error) {
	var g globals
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--json":
			g.jsonOut = true
		case arg == "--socket" || arg == "--root" || arg == "--spool":
			if i+1 >= len(args) {
				return nil, g, fmt.Errorf("%s needs a value", arg)
			}
			i++
			assign(&g, arg, args[i])
		case strings.HasPrefix(arg, "--socket="):
			g.socket = strings.TrimPrefix(arg, "--socket=")
		case strings.HasPrefix(arg, "--root="):
			g.root = strings.TrimPrefix(arg, "--root=")
		case strings.HasPrefix(arg, "--spool="):
			g.spool = strings.TrimPrefix(arg, "--spool=")
		default:
			out = append(out, arg)
		}
	}
	return out, g, nil
}

func assign(g *globals, flagName, value string) {
	switch flagName {
	case "--socket":
		g.socket = value
	case "--root":
		g.root = value
	case "--spool":
		g.spool = value
	}
}

func (c *ctl) print(v any) {
	if c.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(v); err != nil {
			fail(err)
		}
		return
	}
	fmt.Println(human(v))
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "lymphctl: %v\n", err)
	os.Exit(1)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
