package lab

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/ycooi/Lymph/internal/engine"
	"github.com/ycooi/Lymph/internal/objectstore"
	"github.com/ycooi/Lymph/internal/protocol"
)

// Simulator is one fake application in the lab.
//
// The five simulators are deliberately unrelated in what they do and
// deliberately identical in how they look to Lymph: same file names, same
// config-family names, same junction-shaped vocabulary. If Lymph confuses any
// two of them, the lab fails.
type Simulator struct {
	Name        string
	Family      string
	File        string
	Junction    string
	Workflow    string
	Owner       string
	Feedback    protocol.FeedbackType
	Reason      string
	Baseline    objectstore.Bundle
	Description string
}

// Manifest builds the registration for a simulator.
func (s *Simulator) Manifest() engine.Manifest {
	return engine.Manifest{
		Name:  s.Name,
		Type:  "lab-simulator",
		Owner: s.Owner,
		Junctions: []engine.JunctionSpec{
			{
				Name:         s.Junction,
				Workflow:     s.Workflow,
				ConfigFamily: s.Family,
			},
		},
		ConfigFamilies: []engine.ConfigFamilySpec{
			{
				Name:           s.Family,
				SchemaRevision: s.Family + "-v1",
				Workflow:       s.Workflow,
				Targets: []engine.TargetSpec{
					{
						Type:         "SINGLE_FILE",
						Path:         "/tmp/lymph-lab/" + s.Name + "/" + s.File,
						ReloadPolicy: "WATCH_FILE",
						Atomicity:    "FULL",
					},
				},
			},
		},
	}
}

// ---------- MCP simulator: semantic event rules ----------

// SemanticRule is one phrase-to-state rule.
type SemanticRule struct {
	ID     string `yaml:"id"`
	Phrase string `yaml:"phrase"`
	State  string `yaml:"state"`
}

type semanticDoc struct {
	Version int            `yaml:"version"`
	Rules   []SemanticRule `yaml:"rules"`
}

// MCPSim is the SemanticService-shaped simulator: text in, event state out.
func MCPSim() *Simulator {
	return &Simulator{
		Name:        "mcp-sim",
		Family:      "semantic_event_rules",
		File:        "rules.yaml",
		Junction:    "semantic.event_state",
		Workflow:    "semantic-rule-repair",
		Owner:       "desk",
		Feedback:    protocol.FeedbackUnknown,
		Reason:      "UNKNOWN_EVENT_PHRASE",
		Description: "text classifier over trade-news phrasing",
		Baseline: objectstore.Bundle{"rules.yaml": bundle("# SemanticService semantic event rules", semanticDoc{Version: 1, Rules: []SemanticRule{
			{ID: "awarded", Phrase: "awarded", State: "AWARDED"},
			{ID: "purchased", Phrase: "purchased", State: "AWARDED"},
			{ID: "sold", Phrase: "sold", State: "SOLD"},
			{ID: "offered", Phrase: "offered", State: "OFFERED"},
			{ID: "shipped", Phrase: "shipped", State: "SHIPPED"},
		}})},
	}
}

// ParseSemanticRules reads the rules out of a bundle.
func ParseSemanticRules(bundle objectstore.Bundle) ([]SemanticRule, error) {
	var doc semanticDoc
	data, ok := bundleFile(bundle, "rules.yaml")
	if !ok {
		return nil, fmt.Errorf("bundle has no rules.yaml")
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if len(doc.Rules) == 0 {
		return nil, fmt.Errorf("no rules in bundle")
	}
	return doc.Rules, nil
}

// ClassifySemantic applies the rules with a conservative longest-phrase-wins
// policy. This is the application's logic, not Lymph's: Lymph never sees it.
func ClassifySemantic(rules []SemanticRule, text string) string {
	lower := strings.ToLower(text)
	best := ""
	bestLen := 0
	for _, rule := range rules {
		phrase := strings.ToLower(rule.Phrase)
		if phrase == "" || !strings.Contains(lower, phrase) {
			continue
		}
		if len(phrase) > bestLen {
			best, bestLen = rule.State, len(phrase)
		}
	}
	if best == "" {
		return "UNKNOWN"
	}
	return best
}

// SemanticFixtures are the replay corpus for the MCP simulator. The negative
// case is the point of the test: a candidate that widens the rule too far turns
// freight into an award, and must be rejected.
var SemanticFixtures = []struct {
	Text  string
	State string
}{
	{"RCF was awarded 50,000 mt of urea", "AWARDED"},
	{"The business was booked with RCF at $610/t", "AWARDED"},
	{"Freight was booked with ABC Shipping for the parcel", "UNKNOWN"},
	{"Engro purchased 25,000 mt DAP", "AWARDED"},
	{"The cargo shipped from Karachi", "SHIPPED"},
}

// ReplaySemantic validates a candidate bundle against the fixtures.
func ReplaySemantic(bundle objectstore.Bundle) (bool, string) {
	rules, err := ParseSemanticRules(bundle)
	if err != nil {
		return false, "unparseable rules: " + err.Error()
	}
	for _, fixture := range SemanticFixtures {
		got := ClassifySemantic(rules, fixture.Text)
		if got != fixture.State {
			return false, fmt.Sprintf("%q classified as %s, expected %s", fixture.Text, got, fixture.State)
		}
	}
	return true, fmt.Sprintf("%d fixtures classified correctly", len(SemanticFixtures))
}

// ---------- Agent simulator: tool policy ----------

type agentPolicy struct {
	Intent string `yaml:"intent"`
	Tool   string `yaml:"tool"`
}

type agentDoc struct {
	Policies []agentPolicy `yaml:"policies"`
}

// AgentSim routes intents to tools.
func AgentSim() *Simulator {
	return &Simulator{
		Name:        "agent-sim",
		Family:      "policy",
		File:        "rules.yaml",
		Junction:    "agent.tool_policy",
		Workflow:    "agent-policy-improvement",
		Owner:       "ops",
		Feedback:    protocol.FeedbackOutOfContract,
		Reason:      "UNKNOWN_INTENT",
		Description: "intent to tool policy",
		Baseline: objectstore.Bundle{"rules.yaml": bundle("# agent tool policy", agentDoc{Policies: []agentPolicy{
			{Intent: "weather", Tool: "weather_tool"},
			{Intent: "market price", Tool: "price_tool"},
			{Intent: "email", Tool: "email_tool"},
		}})},
	}
}

// ParseAgentPolicies reads the policy table.
func ParseAgentPolicies(bundle objectstore.Bundle) ([]agentPolicy, error) {
	var doc agentDoc
	data, ok := bundleFile(bundle, "rules.yaml")
	if !ok {
		return nil, fmt.Errorf("bundle has no rules.yaml")
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return doc.Policies, nil
}

// ResolveTool answers "which tool for this request" for the simulator.
func ResolveTool(policies []agentPolicy, request string) string {
	lower := strings.ToLower(request)
	for _, policy := range policies {
		if strings.Contains(lower, strings.ToLower(policy.Intent)) {
			return policy.Tool
		}
	}
	return ""
}

// AgentFixtures are known requests plus the new one from the test plan.
var AgentFixtures = []struct {
	Request string
	Tool    string
}{
	{"what is the weather in Region A", "weather_tool"},
	{"market price of urea today", "price_tool"},
	{"email the desk the summary", "email_tool"},
	{"track vessel arrival and notify me when eta changes", "vessel_tracker"},
}

// ReplayAgent checks that every fixture resolves, old behaviour included.
func ReplayAgent(bundle objectstore.Bundle) (bool, string) {
	policies, err := ParseAgentPolicies(bundle)
	if err != nil {
		return false, "unparseable policy: " + err.Error()
	}
	for _, fixture := range AgentFixtures {
		if got := ResolveTool(policies, fixture.Request); got != fixture.Tool {
			return false, fmt.Sprintf("%q resolved to %q, expected %q", fixture.Request, got, fixture.Tool)
		}
	}
	return true, fmt.Sprintf("%d intents resolve", len(AgentFixtures))
}

// ---------- ML simulator: model reference plus serving parameters ----------

type mlDoc struct {
	ActiveModel string  `yaml:"active_model"`
	Threshold   float64 `yaml:"threshold"`
}

// MLSim serves a small classifier whose artifact lives outside Lymph.
func MLSim() *Simulator {
	return &Simulator{
		Name:        "ml-sim",
		Family:      "model",
		File:        "config.yaml",
		Junction:    "model.serving",
		Workflow:    "model-refresh",
		Owner:       "quant",
		Feedback:    protocol.FeedbackPerformanceDrift,
		Reason:      "DISTRIBUTION_DRIFT",
		Description: "classifier with an external model artifact",
		Baseline:    objectstore.Bundle{"config.yaml": bundle("# serving configuration", mlDoc{ActiveModel: "model-v1", Threshold: 0.50})},
	}
}

func parseML(bundle objectstore.Bundle) (mlDoc, error) {
	var doc mlDoc
	data, ok := bundleFile(bundle, "config.yaml")
	if !ok {
		return doc, fmt.Errorf("bundle has no config.yaml")
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return doc, err
	}
	return doc, nil
}

// EvaluateClassifier is a stand-in for real evaluation: v1 was trained on the
// old distribution and degrades as the new one arrives; v2 recovers.
func EvaluateClassifier(model string, threshold float64) (accuracy float64) {
	base := 0.942
	switch model {
	case "model-v1":
		base = 0.712
	case "model-v2":
		base = 0.942
	default:
		return 0
	}
	// A threshold far from the tuned value costs a little accuracy, so a
	// candidate that changes the model but not the threshold is measurably
	// worse than one that changes both.
	penalty := 0.0
	if threshold > 0.45 {
		penalty = 0.031
	}
	if threshold < 0.30 {
		penalty = 0.018
	}
	return base - penalty
}

// ReplayML validates a candidate: it must clear the family's accuracy gate.
func ReplayML(bundle objectstore.Bundle) (bool, string) {
	doc, err := parseML(bundle)
	if err != nil {
		return false, "unparseable config: " + err.Error()
	}
	accuracy := EvaluateClassifier(doc.ActiveModel, doc.Threshold)
	if accuracy < 0.90 {
		return false, fmt.Sprintf("accuracy %.3f is below the 0.900 gate (model %s, threshold %.2f)",
			accuracy, doc.ActiveModel, doc.Threshold)
	}
	return true, fmt.Sprintf("accuracy %.1f%% with %s at threshold %.2f",
		accuracy*100, doc.ActiveModel, doc.Threshold)
}

// ---------- Ingestion simulator: schema mapping ----------

type ingestMapping struct {
	Aliases  map[string]string `yaml:"aliases"`
	Required []string          `yaml:"required"`
}

// IngestSim parses an upstream feed whose column names change without notice.
func IngestSim() *Simulator {
	return &Simulator{
		Name:        "ingest-sim",
		Family:      "default",
		File:        "config.yaml",
		Junction:    "ingest.schema",
		Workflow:    "parser-schema-improvement",
		Owner:       "data",
		Feedback:    protocol.FeedbackSchemaDrift,
		Reason:      "NEW_SOURCE_TABLE_LAYOUT",
		Description: "upstream JSON feed to canonical records",
		Baseline: objectstore.Bundle{"config.yaml": bundle("# upstream column mapping", ingestMapping{
			Aliases:  map[string]string{"price_usd": "price_usd", "quantity_mt": "quantity_mt"},
			Required: []string{"price_usd", "quantity_mt"},
		})},
	}
}

// IngestFixtures are the records the parser must keep handling.
var IngestFixtures = []struct {
	Name string
	Row  map[string]any
	OK   bool
}{
	{"old format", map[string]any{"price_usd": 610, "quantity_mt": 50000}, true},
	{"new format", map[string]any{"price": 610, "volume": 50000, "currency": "USD"}, true},
	{"empty field", map[string]any{"price": 0, "volume": 0, "currency": "USD"}, false},
	{"missing field", map[string]any{"price": 610}, false},
	{"duplicate record", map[string]any{"price_usd": 610, "quantity_mt": 50000}, true},
}

// ReplayIngest applies a mapping to the fixtures.
func ReplayIngest(bundle objectstore.Bundle) (bool, string) {
	var mapping ingestMapping
	data, ok := bundleFile(bundle, "config.yaml")
	if !ok {
		return false, "bundle has no config.yaml"
	}
	if err := yaml.Unmarshal(data, &mapping); err != nil {
		return false, "unparseable mapping: " + err.Error()
	}
	for _, fixture := range IngestFixtures {
		canonical, ok := ApplyMapping(mapping, fixture.Row)
		if ok != fixture.OK {
			return false, fmt.Sprintf("%s: parsed=%v, expected %v", fixture.Name, ok, fixture.OK)
		}
		if ok && (canonical["price_usd"] == 0 || canonical["quantity_mt"] == 0) {
			return false, fmt.Sprintf("%s: canonical record lost a field: %v", fixture.Name, canonical)
		}
	}
	return true, fmt.Sprintf("%d fixtures parse", len(IngestFixtures))
}

// ApplyMapping rewrites one upstream row into the canonical shape.
func ApplyMapping(mapping ingestMapping, row map[string]any) (map[string]any, bool) {
	out := map[string]any{}
	for source, target := range mapping.Aliases {
		value, ok := row[source]
		if !ok {
			continue
		}
		out[target] = value
	}
	for _, field := range mapping.Required {
		value, ok := out[field]
		if !ok {
			return nil, false
		}
		switch typed := value.(type) {
		case int:
			if typed == 0 {
				return nil, false
			}
		case float64:
			if typed == 0 {
				return nil, false
			}
		case string:
			if typed == "" {
				return nil, false
			}
		case nil:
			return nil, false
		}
	}
	return out, true
}

// ---------- Generic service simulator: no AI anywhere ----------

type serviceDoc struct {
	RetryCount int `yaml:"retry_count"`
	TimeoutMS  int `yaml:"timeout_ms"`
}

// ServiceSim is an ordinary daemon with retry and timeout settings.
func ServiceSim() *Simulator {
	return &Simulator{
		Name:        "service-sim",
		Family:      "production",
		File:        "config.yaml",
		Junction:    "service.runtime",
		Workflow:    "service-tuning",
		Owner:       "ops",
		Feedback:    protocol.FeedbackPerformanceDrift,
		Reason:      "UPSTREAM_LATENCY",
		Description: "plain Go service with retry and timeout settings",
		Baseline:    objectstore.Bundle{"config.yaml": bundle("# service runtime settings", serviceDoc{RetryCount: 3, TimeoutMS: 5000})},
	}
}

// BenchmarkService models 200 upstream calls whose latency distribution has
// shifted from 4s to 8s. A short timeout with many retries amplifies the
// problem; a longer timeout with fewer retries absorbs it.
func BenchmarkService(bundle objectstore.Bundle) (successRate float64, p95 int, err error) {
	var doc serviceDoc
	data, ok := bundleFile(bundle, "config.yaml")
	if !ok {
		return 0, 0, fmt.Errorf("bundle has no config.yaml")
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return 0, 0, err
	}
	if doc.TimeoutMS <= 0 || doc.RetryCount < 0 {
		return 0, 0, fmt.Errorf("nonsensical settings: retry_count=%d timeout_ms=%d", doc.RetryCount, doc.TimeoutMS)
	}

	const calls = 200
	upstream := 8000
	successes := 0
	worst := 0
	for call := 0; call < calls; call++ {
		elapsed := 0
		ok := false
		for attempt := 0; attempt <= doc.RetryCount; attempt++ {
			elapsed += upstream
			if upstream <= doc.TimeoutMS {
				ok = true
				break
			}
			// Each retry costs another full upstream latency.
		}
		if ok {
			successes++
		}
		if elapsed > worst {
			worst = elapsed
		}
	}
	return float64(successes) / float64(calls), worst, nil
}

// ReplayService is the benchmark worker's verdict.
func ReplayService(bundle objectstore.Bundle) (bool, string) {
	rate, p95, err := BenchmarkService(bundle)
	if err != nil {
		return false, err.Error()
	}
	if rate < 0.99 {
		return false, fmt.Sprintf("success rate %.3f below 0.99 (worst call %dms)", rate, p95)
	}
	if p95 > 15000 {
		return false, fmt.Sprintf("worst call %dms exceeds the 15000ms budget", p95)
	}
	return true, fmt.Sprintf("success %.1f%%, worst call %dms", rate*100, p95)
}

// ---------- the lab roster ----------

// Simulators returns the five fake applications.
func Simulators() []*Simulator {
	return []*Simulator{AgentSim(), MCPSim(), MLSim(), IngestSim(), ServiceSim()}
}

// Replay runs the simulator's own validation workflow over a candidate bundle.
func (s *Simulator) Replay(bundle objectstore.Bundle) (bool, string) {
	switch s.Name {
	case "agent-sim":
		return ReplayAgent(bundle)
	case "mcp-sim":
		return ReplaySemantic(bundle)
	case "ml-sim":
		return ReplayML(bundle)
	case "ingest-sim":
		return ReplayIngest(bundle)
	case "service-sim":
		return ReplayService(bundle)
	default:
		return false, "unknown simulator"
	}
}

// bundle renders a YAML value with a leading comment line, so every simulator
// ships a file that looks like a real config.
func bundle(header string, value any) []byte {
	raw, err := yaml.Marshal(value)
	if err != nil {
		panic(err)
	}
	text := strings.TrimSpace(header) + "\n" + string(raw)
	return []byte(strings.TrimLeft(text, "\n"))
}

// digestBundle gives a stable summary of a bundle for assertions.
func digestBundle(b objectstore.Bundle) string {
	paths := make([]string, 0, len(b))
	for path := range b {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var b2 strings.Builder
	for _, path := range paths {
		b2.WriteString(path)
		b2.WriteString(":")
		b2.WriteString(strconv.Itoa(len(b[path])))
		b2.WriteString(";")
	}
	return b2.String()
}

// bundleFile picks a named file deterministically. Go map iteration order is
// random, so a parser that shrugged and took "the first file" would make the
// lab flaky instead of strict.
func bundleFile(bundle objectstore.Bundle, name string) ([]byte, bool) {
	if data, ok := bundle[name]; ok {
		return data, true
	}
	if len(bundle) == 1 {
		for _, data := range bundle {
			return data, true
		}
	}
	return nil, false
}
