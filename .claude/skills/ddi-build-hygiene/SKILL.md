---
name: ddi-build-hygiene
description: Keep ddi builds from killing the machine - where cargo's target dir lives (external drive), disk and memory limits, never two heavy builds at once, how to run long commands without blocking. Read before any cargo build/test/clippy in ~/ddi or ~/kdi, and whenever a build is SIGKILLed, a command's output vanishes, or the disk is nearly full.
---

# Build hygiene for ddi (this 16 GB laptop)

What went wrong before, so it does not again:

| Symptom | Cause | Rule |
|---|---|---|
| `rustc ... (signal: 9, SIGKILL)` | two heavy builds at once (ddi tests + kdi release) exhausted RAM | **One heavy cargo job at a time.** Always `-j 2`. A kdi release build alone is fine, ~27 min. |
| tool output "lost ... ENOSPC", `/tmp` "full" | the root disk was full: `~/ddi/target` had grown to **140 GB** (many feature combos x test binaries) | Move the target dir to the external drive (below, once the label is fixed). If the internal disk passes ~85%, `cargo clean` before anything else; free space by deleting build output, never data. |
| VS Code / session killed mid-build | systemd-oomd | `~/.cargo/config.toml` runs rustc and test binaries in `build.slice` via `~/.local/bin/in-build-slice`. Do not bypass it. |
| command "moved to the background" after 120 s | foreground limit | Run anything over ~2 min with `run_in_background`, and wait with a Bash `until`-loop in the background (or Monitor), not `sleep`. Redirect big logs to the scratchpad, filter with `grep`, never `cat` them. |

## The external drive (prepared, not active yet)
Goal: cargo's target dir on the USB drive (466 GB exFAT, ~90 MB/s, executables run from it, **no hard or symbolic links**: cargo copies instead). Status 2026-10-13: **blocked by the drive's label**. It mounts at `/media/lutz/External HD`, and the space breaks librdkafka's configure script (`MKL_OUTDBG: ambiguous redirect`, "No such file: /media/lutz/External"). A symlink does not help (the build script sees the real path). Everything else is in place:
- `~/.cargo/config.toml` has the `target-dir = "/home/lutz/cargo-target"` line commented out; `~/cargo-target` is a symlink to `<mount>/cargo-target`.
- `~/.claude/hooks/cargo-needs-drive.sh` (PreToolUse on Bash, beside the rtk hook) blocks cargo/rustc/rustup with a "plug in the drive" message when that line is active and the symlink's mount is absent.
- **To activate:** the user renames the drive's label to something without a space (unmount it in Files, rename, replug; or `sudo exfatlabel /dev/sdb1 ExternalHD`). Then: `mkdir -p /media/lutz/ExternalHD/cargo-target && ln -sfn /media/lutz/ExternalHD/cargo-target ~/cargo-target`, uncomment the line in `~/.cargo/config.toml`, and run `cargo check --all-targets --features kafka,fault-injection -j 2` once (the librdkafka build is the real test; ~15 min). Update the old absolute path in memory `cargo-target-on-external-hd` and remove this paragraph's "blocked" status.
- Until then builds go to `~/ddi/target` on the internal disk: watch `df -h /`, `cargo clean` past ~85%, and expect it to reach 100+ GB if every feature combination is built.
- When active and the drive is unplugged: **tell the user to plug it in**; never point `CARGO_TARGET_DIR` at the internal disk to get around the hook.

## Commands
Prefix: `export PATH="$HOME/.cargo/bin:$PATH"` (cargo is not on the non-interactive PATH).
Gate and feature matrix: see CLAUDE.md. Check disk before a big run: `df -h / "/media/lutz/External HD" | tail -2`.

## kdi (the other repo, `~/kdi`)
`CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 CARGO_PROFILE_RELEASE_DEBUG=false cargo build --release --features azure -j 2` (it refuses to build without the `azure` or `s3` feature). Copy the binary out of `target/` before cleaning: `~/kdi-private/bin/`.
