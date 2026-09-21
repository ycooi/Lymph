# Use Cases

Lymph is designed for software whose configuration is expected to keep evolving
after deployment.

This is common in AI and agent-based systems. An application may be useful
enough to launch while prompts, routing rules, entity mappings, classification
rules, retrieval profiles, evidence policies, thresholds, normalizers, and
domain terminology are still improving through real-world use.

The challenge is not that these systems evolve. The challenge is preserving
the evidence behind that evolution:

- What keeps producing inadequate results?
- Which configuration revision was active at the time?
- Is the same problem recurring?
- Why was a change originally made?
- Did the next revision solve the problem that motivated it?
- Where are people repeatedly correcting the system?
- Which configuration families are stabilizing, and which are not?

Git records what changed. Logs record what happened. Monitoring shows whether
the system is healthy. Tracing shows how a request moved through the system.
Lymph addresses a different question:

> **What is production teaching us about how this application's evolving
> configuration needs to improve?**

## Junctions

A **junction** is a place where an application knows that its current
configuration may continue evolving through real-world use. Examples include:

```text
classification.outcome
entity.resolve
routing.selection
retrieval.coverage
parser.document
policy.evidence
normalization.product
```

The application does not send every operation to Lymph. It reports meaningful
cases where production exposed a limitation in an evolvable configuration:

```text
UNKNOWN_PHRASE
UNKNOWN_ENTITY
AMBIGUOUS_ROUTE
EVIDENCE_COVERAGE_LOW
HUMAN_CORRECTION
UNKNOWN_DOCUMENT_LAYOUT
```

A feedback event can identify what happened, where and why it happened, which
input exposed it, and which configuration revision was active. Lymph preserves
that event and groups repeated observations into an issue.

```text
configuration r17
       │
   production
       │
 unexpected or inadequate behavior
       │
    junction
       │
    feedback
       │
 grouped occurrences
       │
      issue
```

Lymph keeps this history even when no one decides to change the configuration.

## 1. Evolving classification rules

An application may classify market news, documents, tickets, products,
transactions, or user requests through configuration-driven rules.

```text
classification_rules:v12
        │
        ├── known cases work
        └── production discovers new language
                    │
                    ▼
          classification.outcome
                    │
             UNKNOWN_PHRASE
```

Rather than silently adding another rule, the application can report the
inadequacy through a junction:

```text
junction            classification.outcome
feedback            UNKNOWN
reason              UNKNOWN_PHRASE
config revision     classification_rules:v12
input reference     doc://...
```

The accumulated evidence may later inform a manual change, an agent-generated
proposal, or another improvement workflow. Lymph does not require a particular
repair mechanism.

## 2. Entity and alias resolution

Real-world entities rarely use one consistent name. A supplier, product, or
organization can appear under legal names, abbreviations, misspellings, and
local conventions that were absent from the original alias map.

```text
entity_aliases:v7
      │
unknown mention
      │
entity.resolve
      │
UNKNOWN_ENTITY_MENTION
```

Lymph keeps each observation tied to the exact alias configuration active at
the time, making repeated gaps visible without deciding which alias is correct.

## 3. Agent routing and tool selection

Agents often use configurable routing to select a search service, database,
calculator, specialist agent, workflow, or human escalation path.

```text
user request
     │
   router
 ┌───┼──────────┬────────────┐
search database calculator specialist/human
```

Production can reveal request shapes that were not anticipated when the router
was designed. Application-defined feedback might include:

```text
UNKNOWN_ROUTE
AMBIGUOUS_ROUTE
WRONG_TOOL_SELECTED
HUMAN_OVERRIDE
```

The issue history shows where the policy remains weak and whether later
revisions reduce the same signals.

## 4. Prompt evolution

Prompts are often production configuration. Without structured evidence, a
prompt's history can degrade into commit messages such as "fix edge case" or
"better formatting."

Lymph can connect runtime observations to the prompt revision that produced
them:

```text
prompt:v31                 prompt:v32
  47 FORMAT_FAILURE          8 FORMAT_FAILURE
  19 MISSING_EVIDENCE       17 MISSING_EVIDENCE
  12 HUMAN_CORRECTION        4 HUMAN_CORRECTION
```

These counts are evidence, not an automatic verdict that one prompt is better.
They make the change reviewable in the context of what motivated it.

## 5. Retrieval and evidence policies

Retrieval systems commonly contain evolving source requirements, freshness
thresholds, ranking weights, document classes, minimum evidence counts, and
fallback behavior.

Production may reveal signals such as:

```text
NO_EVIDENCE_FOR_CLAIM
EVIDENCE_COVERAGE_LOW
SOURCE_TOO_OLD
WRONG_DOCUMENT_CLASS
```

A team can use the resulting evidence to adjust a threshold, change ranking,
add a source, revise fallback behavior, or leave the policy unchanged. Lymph
records the evidence; it does not make the policy decision.

## 6. Human corrections as learning signals

A repeated human override is a strong signal that configuration may need
attention.

```text
system result
     │
human correction
     │
   junction
     │
HUMAN_CORRECTION
```

Instead of disappearing into chats, tickets, logs, or operational memory,
corrections become structured observations. They can expose a missing alias, a
weak prompt, an incorrect threshold, or an incomplete rule. They never change
configuration automatically merely because they were recorded.

## 7. Schema and document interpretation

Applications ingesting external data encounter formats outside their control:
supplier spreadsheets, shipping documents, PDF reports, APIs, emails, EDI
messages, and CSV exports.

```text
parser_rules:v18
      │
new document variant
      │
parser.document
      │
COLUMN_MISSING / TYPE_CHANGED / UNKNOWN_LAYOUT
```

Lymph can track which parser configuration encountered the structure and how
often similar cases recur.

## 8. Domain knowledge that evolves through use

Many applications keep lightweight domain knowledge in configuration:

```text
aliases             product taxonomies
country mappings    industry terminology
unit normalization  status mappings
business rules      synonyms and qualifiers
```

These definitions are rarely complete on day one. Lymph provides a common way
to preserve unknown and ambiguous cases as the application's knowledge evolves.

## From configuration history to evolution history

Git can show that `routing.yaml` changed from `r12` to `r13`. Lymph can preserve
the production context around those revisions:

```text
r12                         r13
 ├── 83 UNKNOWN_ROUTE        ├── 19 UNKNOWN_ROUTE
 ├── 21 AMBIGUOUS_ROUTE      ├── 11 AMBIGUOUS_ROUTE
 └── 14 HUMAN_CORRECTION     └──  8 HUMAN_CORRECTION
```

That context helps a team ask which problems recur, which revision reduced a
particular signal, which changes introduced new problems, and which
configuration surfaces still require frequent human intervention. It does not
prove causality on its own; it makes the supporting evidence inspectable.

## When to create a junction

> **Create a junction where production usage can teach you that an existing
> configuration should evolve.**

Good candidates include `classification.*`, `entity.*`, `routing.*`,
`retrieval.*`, `parser.*`, `schema.*`, `policy.*`, `prompt.*`, and
`normalization.*`.

A junction should not exist merely because something can fail. Disk exhaustion,
network timeouts, database outages, expired certificates, and process crashes
normally belong in logging, monitoring, tracing, or incident management.

Lymph is for the question: where is real-world usage showing that the
application's current configuration no longer understands the world well
enough?

## Lymph is not the repair system

What happens after feedback becomes an issue is intentionally a separate
choice:

```text
                         ┌── engineer changes configuration
                         ├── GitHub issue or pull request
Lymph issue ─────────────┼── human review
                         ├── agent proposes a candidate
                         └── no action
```

Lymph can preserve candidates, validations, approvals, and human-gated pull
delivery. It does not autonomously edit an application's files, reload a
service, or deploy a change. See [Production](PRODUCTION.md) for the exact
supported boundary.

> **Ship the MVP before you know every edge case. Just don't lose what
> production teaches you.**
