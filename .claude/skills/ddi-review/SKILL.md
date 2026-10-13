---
name: ddi-review
description: Review a ddi branch, PR or milestone before merging with a bounded budget - one round, scope guard, verified findings only, no endless review loops. Use when the user asks to review, "run the review", or check a ddi change (issues PRs, Kafka milestones M0-M6) before PR/merge.
---

# ddi review: one bounded round

Past reviews (issues #10-13: rounds of 19, 13, 13 confirmed findings; Kafka M3: 3 rounds) burned quota
because each round found bugs in code the previous round's fixes had added, and in add-ons nobody asked for.
This skill caps the loop. Rules of the project: [[CLAUDE.md]] "Rules already settled".

## 0. Fix the scope before anyone reads code
- Base = `git merge-base HEAD origin/main` (or the milestone's start commit). Diff = `git diff base...HEAD`.
- Write down the **contract**: the issue texts (`gh issue view N`) or the milestone's rows in `docs/kafka-design.md` §13 and the invariants (§4.1, I1..I18) it touches. Findings are judged against this contract only.
- List add-ons: changes not required by the contract. Flag them to the user now; the default is to move them to their own branch (`feat/<name>`) instead of reviewing them.

## 1. Screen, then read (cheap before expensive)
Use the `typesafe-offload` skill: split the diff by hunk/file, and rank hunks with one yes/no per risk
question, e.g. "does this change commit/offset/fence/txn logic?", "does it touch error classification or
shutdown/rebalance paths?", "does it change what is written to a Delta table or a DQ table?", "does it
alter a Trino-dialect function's output?". Give agents the top-ranked hunks plus the contract, not the
whole tree. Spot-check 5 low-ranked hunks yourself.

## 2. One review pass, lenses chosen by what the diff touches (max 4, at most 2 agents at once on this laptop)
- **Exactly-once / data loss** (anything under `src/kafka`, `fence.rs`, `sink.rs`, `dedup.rs`, `upsert.rs`): can a row be lost, duplicated, or committed over a moved offset? Failure matrix §4.7.
- **Concurrency and lifecycle**: rebalances, shutdown, held batches, pause/resume/seek, tasks that outlive their owner.
- **Trino/Starburst parity** (`src/transform`, `dbt/`): verified with `ddi-trino-check`, not from memory.
- **Regression and test quality**: Delta-only build unchanged (I13/I16), tests that would pass without the fix (mutate the fix and watch it fail), CI-only paths (workflow YAML, Dockerfile) that no local run covers.
Each finding must carry: file:line, a concrete failing input/state, and the contract clause it breaks.
Without file:line plus a failure scenario, it is not a finding.

## 3. Verify, then triage (this is where the quota is saved)
- Reproduce every blocker/major finding with a test or a trace before accepting it (independent agent or you; not the reporter). Unreproduced = dropped, and say so.
- Never merge duplicates by file/line bucket without keeping each member.
- Classify: **in-contract correctness bug** -> fix; **out-of-contract / add-on / heuristic** -> do not fix, list as follow-up; **style/minor** -> fix only if trivial, never loop on it.
- A finding in code written *to fix an earlier finding*: replace that heuristic with a mechanism that is exact by construction, or remove the optimisation (do not add a third patch).

## 4. Fix and stop
- Fix confirmed in-contract bugs, each with the test that fails before and passes after, one commit per finding (`fix: <what, as the user sees it>`), gate from CLAUDE.md.
- **No second full round.** Re-check only the files the fixes touched, once, with a single agent. If that finds a new correctness bug, fix it; if it finds anything else, list it and stop.
- Budget: say up front how many agents and rough token use; ask before exceeding ~10 agents or a second round.

## 5. Report (to the user, short)
Contract reviewed; hunks screened/read; findings by verdict (confirmed+fixed, confirmed+deferred, refuted/unreproduced); add-ons split out; what was NOT reviewed (e.g. soak, M-1 data); anything that still needs the user's decision. For milestone reviews, append the findings to the "as built" section of `docs/kafka-design.md` in the order reported, as earlier milestones did.
