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

## The external drive (ACTIVE since 2026-10-13)
cargo's target dir is `~/cargo-target` -> `/media/lutz/cargotarget`, a **150 GB sparse ext4 image** (`/media/lutz/ExternalHD/cargo-target.img`) loop-mounted through udisks. Why an image: the drive is exFAT (no symlinks), and librdkafka's `make` does `ln -s librdkafka.so`; the label also had a space (now `ExternalHD`). No sudo needed anywhere.
- `~/.cargo/config.toml` has `target-dir = "/home/lutz/cargo-target"` active. Verified: `cargo check --all-targets --features kafka,fault-injection -j 2` from a cold target dir passed in 5m18s.
- **After a reboot or replug the image is not mounted.** Re-attach: `udisksctl loop-setup -f /media/lutz/ExternalHD/cargo-target.img` (prints /dev/loopN), then `udisksctl mount -b /dev/loopN`. Mount point is `/media/lutz/cargotarget` (label `cargotarget`).
- `~/.claude/hooks/cargo-needs-drive.sh` blocks cargo/rustc/rustup unless the symlink target is a mount point, and says how to re-attach. Never point `CARGO_TARGET_DIR` at the internal disk to get around it.
- Image full (148 GB)? `cargo clean` inside it; it is regenerable.

## Commands
Prefix: `export PATH="$HOME/.cargo/bin:$PATH"` (cargo is not on the non-interactive PATH).
Gate and feature matrix: see CLAUDE.md. Check disk before a big run: `df -h / "/media/lutz/External HD" | tail -2`.

## kdi (the other repo, `~/kdi`)
`CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 CARGO_PROFILE_RELEASE_DEBUG=false cargo build --release --features azure -j 2` (it refuses to build without the `azure` or `s3` feature). Copy the binary out of `target/` before cleaning: `~/kdi-private/bin/`.
