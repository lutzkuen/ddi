---
name: ddi-ship
description: Open a PR for the current ddi branch, wait for CI, merge it and optionally cut a release (version bump, tag, image publish) - the recurring "do the PR, merge it, release" request. Use when the user asks to push, open/merge a PR, or make/cut a release in ~/ddi.
---

# Ship ddi: PR -> CI -> merge -> release

Only do the steps the user asked for ("make a PR" is not "merge"; "merge" is not "release").
Never force-push, never skip hooks, never merge with red CI.

## 0. Gate locally (see CLAUDE.md for flags)
fmt, clippy (with and without `--features kafka`), full tests (both configurations), `cargo deny check licenses` if dependencies changed. Run long commands in the background with `timeout`. Do not push on red.

## 1. PR
- `git status` clean, branch is not `main`, rebased on current `origin/main` if it diverged (`git fetch && git rebase origin/main`; resolve conflicts, re-run the gate).
- `git push -u origin <branch>`; `gh pr create --title ... --body ...`. Body: what and why per issue (`Closes #N`), behaviour changes called out, what was verified (tests, Trino, Redpanda), and the PR-description attribution line from the session reminder.
- Keep scope to what the issues ask. If the branch carries add-ons, flag it before opening.

## 2. CI (8 jobs; do not poll in the foreground)
Jobs: fmt/clippy/tests, clippy+tests (kafka), container image, image (Delta-only), Kafka against Redpanda, licence policy, converted SQL in Trino, object storage (Azurite), dbt end-to-end.
Wait with a background command: `gh pr checks <n> --watch --fail-fast` (run_in_background), and act on the notification. On failure: `gh run view --log-failed`, fix, push, repeat. Fix the cause; do not rerun until green or loosen a test.

## 3. Merge (only when asked)
`gh pr merge <n> --merge --delete-branch` (the repo uses merge commits, e.g. "Merge pull request #14 from ..."). Then `git checkout main && git pull`.

## 4. Release (only when asked)
Semver: breaking behaviour change -> minor while 0.x (0.3.1 -> 0.4.0); fix only -> patch. Previous: `git tag -l 'v*'`.
1. On `main`, one commit `chore: release X.Y.Z` touching exactly: `Cargo.toml` (`version`), `Cargo.lock` (the `delta-delta-ingest` entry; run `cargo check -j 2` to refresh it), and the `docker pull ghcr.io/lutzkuen/ddi:X.Y.Z` line in `USING_DDI.md`. CI's image job fails if the tag and Cargo.toml version differ.
2. Push `main`, then an annotated tag whose message is `vX.Y.Z` followed by a blank line and a short changelog grouped by issue, like `git cat-file -p v0.4.0` (one paragraph per issue; call out behaviour changes that affect running models). `git tag -a vX.Y.Z -F msg.txt && git push origin vX.Y.Z`.
3. Watch the tag's CI run in the background (`gh run list --branch vX.Y.Z --limit 1`, then `gh run watch <id> --exit-status`). It publishes `ghcr.io/lutzkuen/ddi:X.Y.Z` and `:latest`.
4. `gh release create vX.Y.Z --title "ddi vX.Y.Z" --notes ...` (same changelog; earlier releases are on the releases page for style).
5. Report: PR number, merge commit, tag, release URL, image tags, and that the tag run was green.

## Don'ts
- No release from a branch other than `main`. No tag before the release commit is pushed.
- Do not touch `dat_kafka_connect` (separate work repo, pins ddi by image/commit).
