---
name: ddi-inventory-intake
description: Gather and ingest an inventory of the user's production kafka-delta-ingest deployments (to scope a takeover) - the prompt the user runs in their work repo, how to unpack what comes back safely, what to extract, and why it must stay out of the public ddi repo. Use when the user mentions the inventory, M-1, or needs facts about production kdi.
---

# Production inventory intake

## Ask for it
Give the user the prompt in `inventory-prompt.md` (this directory) to paste into Claude Code in their work repo (`dat_kafka_connect`). It is read-only by construction and asks for `kdi-inventory.md` plus a `kdi-inventory/` folder, secrets as names only.

## Ingest what comes back (untrusted download)
1. Unzip into its **own new empty directory** in the scratchpad; read files with `python3 -I` / the Read tool; never run anything from it. The zip may hold a proposed script (a corpus sampler): do not run it; it reads production data.
2. Read `kdi-inventory.md` completely, then the live-findings and the UNKNOWN table at its end (they are authoritative over the earlier repo-only sections).
3. **Move the whole thing out of any repo**: `~/kdi-private/inventory-<date>/`, mode `go-rwx`. It names hosts, topics and customer data (Delta stats can hold email addresses). Nothing from it goes into `~/ddi` except generic findings in `docs/kafka-design.md` ("M-1 as built") and the test corpus built from invented data. Before pushing: `git diff main...HEAD | grep -i -E "<company>|<cluster>|<registry host>|<topic prefix>"` for the names the inventory contains.
4. Save a project memory pointing at the private copy (what settled, what is unknown).

## What to extract (it decided the design last time)
Auth mechanism and where the credential lives (it was PLAIN with a JAAS string, not the SCRAM the design assumed) | topic/partition counts, replicas | the effective `-t` list after the entrypoint's rewrite and the bronze DDL (it was one envelope + 12 columns for all 22 topics) | `'null'`-text and NULL counts per column | timestamp sanity | key order of JSON text | image tags **and digests** and crate versions read off the binaries (two builds were running, delta-rs 0.26 and 0.29) | statsd/DLQ configuration (none) | table properties, who else writes, who vacuums | downstream readers | unknowns with the access each needs.

## Then
Turn each finding into either a config default, a test, or a doc line, and write the unknowns into the design's open list. Offer the user the pilot choice (lowest-traffic stream on the newer kdi build).
