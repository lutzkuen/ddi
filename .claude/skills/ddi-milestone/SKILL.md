---
name: ddi-milestone
description: How to carry a docs/kafka-design.md milestone (or any large designed feature) from plan to merged-ready - build against the real counterpart, keep the design's "as built" record honest, commit in reviewable steps, and stop at what only the user can do. Use when asked to "work through the rest of the plan" or to build M4/M5-style work.
---

# Carrying a milestone

The milestone table is `docs/kafka-design.md` §13; each finished milestone has an "**Mx as built**" paragraph. Follow that shape.

1. **Re-read the as-built notes of the previous milestones and the section the new one implements** (`grep -n "as built" docs/kafka-design.md`). Decisions there are settled; do not reopen them.
2. **Inputs first.** If a milestone needs facts only the user has (M-1), ask for them with `ddi-inventory-intake`, then change the plan to match what they say. A finding that contradicts the design (PLAIN not SCRAM, one envelope for all topics) is a design change, written down.
3. **Build the smallest vertical slice, test it, commit it.** One feature per commit with its tests (`feat: ...` / `test: ...` / `docs: ...`, one sentence, the why in the body). Use the gate in CLAUDE.md before each push, not after each edit.
4. **Test against the real thing, not a stub, wherever one can run locally**: Redpanda for the broker (`ddi-kafka-tests`), real kdi for kdi parity (`ddi-kdi-parity`), real Trino for SQL (`ddi-trino-check`). The real-kdi run found a bug no unit test would have. If the real thing is unavailable (the production image, real brokers' SASL), say so in the PR and the design, and name what the stand-in does and does not prove.
5. **Keep the record honest.** For every milestone paragraph list: what was built, where it differs from the design and why, and **what was not built**. Never describe an unrun test as passing. If a claim is an inference (e.g. which avro version production uses), say "by construction" and how it was checked.
6. **One review round** (`ddi-review`), then a full regression of the earlier milestones' integration tests, because shared helpers (`tools::view`) change under them.
7. **Stop at the user's edge.** Pilots against their cluster, pushing/merging/releasing (`ddi-ship`), and anything touching their production are theirs; prepare the runbook, the commands and the checks, and hand over.
8. **Keep private data out of the public repo** (names, hosts, customer rows): generic findings only.
