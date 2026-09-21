package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ycooi/Lymph/internal/buildinfo"
	"github.com/ycooi/Lymph/internal/identity"
	"github.com/ycooi/Lymph/internal/projection"
	"github.com/ycooi/Lymph/pkg/client"
)

// Session and handshake commands. These exercise exactly the protocol an
// application SDK uses, so an operator can debug integration without writing
// code (mission sections 45 to 47, 78).

func (c *ctl) cmdSessions(args []string) {
	fs := newFlagSet("sessions")
	app := fs.String("application", "", "filter by application")
	installation := fs.String("installation", "", "filter by installation")
	state := fs.String("state", "", "ACTIVE, CLOSED, STALE or REJECTED")
	limit := fs.Int("limit", 25, "maximum sessions")
	_ = fs.Parse(args)
	var out struct {
		Sessions []projection.Session `json:"sessions"`
	}
	path := "/v1/sessions" + client.Query("application", *app, "installation", *installation,
		"state", strings.ToUpper(*state), "limit", fmt.Sprint(*limit))
	if err := c.client.Get(context.Background(), path, &out); err != nil {
		fail(err)
	}
	c.print(out.Sessions)
}

func (c *ctl) cmdSession(args []string) {
	fs := newFlagSet("session")
	rest := parseWithPositionals(fs, args)
	if len(rest) != 1 {
		fail(errors.New("usage: lymphctl session SESSION_ID"))
	}
	var session projection.Session
	if err := c.client.Get(context.Background(), "/v1/sessions/"+rest[0], &session); err != nil {
		fail(err)
	}
	c.print(session)
}

// cmdHello performs a handshake and stops there. It is the diagnostic an
// operator reaches for when an application cannot connect: it uses the same
// protocol, the same validation and the same reason codes.
func (c *ctl) cmdHello(args []string) {
	fs := newFlagSet("hello")
	app := fs.String("application", "", "application UUID or registered name")
	installation := fs.String("installation", "", "installation UUID or name")
	process := fs.String("process", "", "process UUID (default: a fresh one)")
	clientName := fs.String("client-name", "lymphctl", "client name to report")
	clientVersion := fs.String("client-version", buildinfo.Version, "client version to report")
	manifestHash := fs.String("manifest-hash", "", "manifest hash to report")
	timeout := fs.Duration("timeout", 2*time.Second, "handshake timeout")
	_ = fs.Parse(args)

	if *app == "" {
		fail(errors.New("hello requires --application"))
	}
	// Resolve names to UUIDs the same way an application would have to, but
	// never guess: an unregistered name is an error, not a new identity.
	appID, installationID, err := c.resolveIdentity(*app, *installation)
	if err != nil {
		fail(err)
	}
	processID := *process
	if processID == "" {
		processID = identity.NewID()
	}

	probe := client.New(client.Options{
		Socket:           c.socket,
		ApplicationID:    appID,
		InstallationID:   installationID,
		ProcessID:        processID,
		ClientName:       *clientName,
		ClientVersion:    *clientVersion,
		ManifestHash:     *manifestHash,
		HandshakeTimeout: *timeout,
		Timeout:          *timeout,
	})
	ctx, cancel := context.WithTimeout(context.Background(), *timeout+time.Second)
	defer cancel()
	response, err := probe.Hello(ctx)
	if err != nil {
		var rejected *client.HandshakeRejectedError
		if errors.As(err, &rejected) {
			fmt.Fprintf(os.Stderr, "handshake rejected: %s: %s\n", rejected.ReasonCode, rejected.Message)
			os.Exit(2)
		}
		fail(err)
	}
	c.print(response)
}

// resolveIdentity turns an application/installation reference into UUIDs.
// Names are convenience; identity is the UUID the daemon validates.
func (c *ctl) resolveIdentity(application, installation string) (string, string, error) {
	ctx := context.Background()
	appID := application
	if !identity.Valid(appID) {
		var out struct {
			Applications []projection.Application `json:"applications"`
		}
		if err := c.client.Get(ctx, "/v1/applications", &out); err != nil {
			return "", "", err
		}
		found := ""
		for _, app := range out.Applications {
			if app.Name == application {
				found = app.ApplicationID
			}
		}
		if found == "" {
			return "", "", fmt.Errorf("no registered application named %q", application)
		}
		appID = found
	}

	var installations struct {
		Installations []projection.Installation `json:"installations"`
	}
	if err := c.client.Get(ctx, "/v1/installations"+client.Query("application", appID), &installations); err != nil {
		return "", "", err
	}
	if installation == "" {
		// Omitting the installation means "the application's default", which for
		// a single-deployment application is the only one there is.
		if len(installations.Installations) == 1 {
			return appID, installations.Installations[0].InstallationID, nil
		}
		return "", "", fmt.Errorf("application has %d installations; pass --installation",
			len(installations.Installations))
	}
	if identity.Valid(installation) {
		return appID, installation, nil
	}
	for _, candidate := range installations.Installations {
		if candidate.Name == installation {
			return appID, candidate.InstallationID, nil
		}
	}
	return "", "", fmt.Errorf("no installation named %q for this application", installation)
}

// cmdIdentityExport writes the identity document an application needs. It is not
// a secret: it is the pair of UUIDs the daemon already knows.
func (c *ctl) cmdIdentityExport(args []string) {
	fs := newFlagSet("identity export")
	app := fs.String("application", "", "application UUID or registered name")
	installation := fs.String("installation", "", "installation UUID or name")
	out := fs.String("out", "", "write to this file instead of stdout")
	_ = fs.Parse(args)

	if *app == "" {
		fail(errors.New("identity export requires --application"))
	}
	appID, installationID, err := c.resolveIdentity(*app, *installation)
	if err != nil {
		fail(err)
	}
	document := map[string]string{
		"application_id":  appID,
		"installation_id": installationID,
	}
	if *out == "" {
		c.print(document)
		return
	}
	raw, err := jsonMarshalIndent(document)
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(*out, raw, 0o640); err != nil {
		fail(err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s\n", *out)
}

func (c *ctl) cmdIdentity(args []string) {
	if len(args) == 0 || args[0] != "export" {
		fail(errors.New("usage: lymphctl identity export --application APP [--installation INST] [--out FILE]"))
	}
	c.cmdIdentityExport(args[1:])
}
func jsonMarshalIndent(v any) ([]byte, error) {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}
