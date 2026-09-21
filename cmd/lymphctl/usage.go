package main

import "fmt"

func usage() {
	fmt.Print(usageText)
}

const usageText = `lymphctl — Lymph control plane

usage: lymphctl [--socket PATH] [--root DIR] [--json] <command> [flags]

inspection
  status                     daemon summary
  health                     liveness
  apps                       registered applications
  junctions                  declared junctions
  families                   config families
  targets                    managed deployment targets
  events [--application A]   recent feedback events
  issues [--status S]        grouped issues, highest occurrence first
  work [--state S]           work items
  refs / reflog / audit      configuration history
  installations              deployments of each application
  sessions / session ID      live runtime sessions
  results / result ID        worker returns (ImprovementResult)
  artifacts [--kind K]       what workers produced
  artifact ID                one artifact

improvement
  return-result WORK_ID --worker NAME --attempt N --file result.json
                             one atomic typed worker return

integration
  hello --application A [--installation I]
                             perform the handshake and show the answer
  identity export --application A [--installation I] [--out FILE]
                             write the identity file an application needs

feedback
  emit --app A --junction J --type UNKNOWN --reason CODE [--payload JSON]

registration
  register-app --file manifest.json

configuration
  baseline   --app A --family F (--file PATH | --dir DIR) [--author X]
  revision   --app A --family F (--file PATH | --dir DIR) --message MSG [--set-ref active]
  revisions  --app A --family F
  lineage    REVISION_ID          how production got here, newest first
  diff       REVISION_A REVISION_B
  candidate  --app A --family F (--file PATH | --dir DIR) --explanation MSG [--base REV]
  candidates --app A --family F [--state S]
  validate   --candidate ID --kind structural|replay|regression|holdout --result PASS|FAIL
  approve    --candidate ID --approver NAME [--comment TEXT]
  promote    --candidate ID --actor NAME
  ref set    --app A --family F --name active --revision ID [--expect ID]

maintenance
  ledger verify              recompute the record hash chain
  rebuild                    drop the SQLite projection and replay the ledger
  object get HASH [--out FILE]

Deployment adapters (L3) are deliberately not implemented yet.
`
