#!/usr/bin/env python3
"""Regenerate THIRD_PARTY_NOTICES.md from Cargo.lock.

MIT and Apache-2.0 both require that copyright and licence notices survive
distribution. Shipping source satisfies that; shipping a binary or a container image
does not, unless the notices travel with it.

Licences are read from the crates vendored under the local cargo registry, so run this
after a build that has fetched every dependency:

    cargo fetch && python3 scripts/third-party-notices.py
"""
import collections
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
REGISTRY = pathlib.Path.home() / ".cargo" / "registry" / "src"
SELF = "delta-delta-ingest"

# Third-party C code a crate compiles and links statically. cargo-deny and the crate table see
# the crate's own licence, not this, so each one's licence files are appended verbatim, for
# every version of the crate in the lockfile (optional ones included: the lockfile lists them,
# and an image built with the feature ships them).
#
# Every crate whose build script links a static C library (`cargo:rustc-link-lib=static=` in
# target/*/build/*/output) is here, except blake3 and psm, whose assembly is their own and
# under their crate licence. Check that list again when the lockfile gains a `-sys` crate.
C_LIBRARIES = [
    (
        "rdkafka-sys",
        "librdkafka",
        ["librdkafka/LICENSES.txt"],
        "librdkafka (BSD-2-Clause), and the third-party code it bundles, each under its own "
        "licence below. Linked by the `kafka` feature.",
    ),
    (
        "zstd-sys",
        "zstd",
        ["zstd/LICENSE"],
        "zstd, offered under BSD-3-Clause or GPLv2; the BSD licence is the one taken.",
    ),
    (
        "aws-lc-sys",
        "AWS-LC",
        ["aws-lc/LICENSE"],
        "AWS-LC, the cryptography rustls uses through aws-lc-rs: Apache-2.0 or ISC for its own "
        "code, with the BoringSSL and OpenSSL/SSLeay notices it carries. Linked by every build.",
    ),
    (
        "ring",
        "ring",
        ["LICENSE", "LICENSE-other-bits", "LICENSE-BoringSSL"],
        "ring's C and assembly, partly from BoringSSL (Apache-2.0 or ISC, as each file says), "
        "used by the object-store client. Linked by every build.",
    ),
    (
        "liblzma-sys",
        "liblzma",
        ["xz/COPYING", "xz/COPYING.0BSD"],
        "liblzma from XZ Utils, bundled and linked statically by every build: 0BSD, as XZ "
        "Utils' COPYING says for liblzma (the tools and scripts it names are not linked).",
    ),
]


def crates_from_lock() -> list[tuple[str, str]]:
    lock = (ROOT / "Cargo.lock").read_text()
    return re.findall(r'^name = "([^"]+)"\nversion = "([^"]+)"', lock, re.M)


def crate_dir(name: str, version: str) -> pathlib.Path | None:
    for src in REGISTRY.glob("*"):
        d = src / f"{name}-{version}"
        if d.is_dir():
            return d
    return None


def c_library_sections(crates: list[tuple[str, str]]) -> tuple[list[str], list[str]]:
    """The appendix of statically linked C code, and what could not be read for it."""
    out, missing = [], []
    for name, label, rels, what in C_LIBRARIES:
        for crate, version in sorted(crates):
            if crate != name:
                continue
            d = crate_dir(crate, version)
            texts = [(rel, (d / rel) if d else None) for rel in rels]
            absent = [rel for rel, text in texts if text is None or not text.exists()]
            if absent:
                missing += [f"{crate} {version} ({rel})" for rel in absent]
                continue
            # The upstream version is the build metadata: `4.10.0+2.12.1`, `2.0.16+zstd.1.5.7`.
            # A crate without it does not say which upstream release it bundles.
            if "+" in version:
                upstream = version.split("+", 1)[1].removeprefix(f"{label}.")
                header = f"### {label} {upstream} (in {crate} {version})"
            else:
                header = f"### {label} (in {crate} {version})"
            out += ["", header, "", what]
            for rel, text in texts:
                body = text.read_text().rstrip()
                # A fence longer than any run of backticks in the text, which may hold its own.
                fence = "`" * max(3, 1 + max((len(r) for r in re.findall(r"`+", body)), default=0))
                if len(texts) > 1:
                    out += ["", f"`{rel}`:"]
                out += ["", f"{fence}text", body, fence]
    if out:
        out = [
            "",
            "## Statically linked C libraries",
            "",
            "The table above lists crates. These are the third-party C libraries some of them",
            "compile and link statically, whose licences travel with the binary too: every one",
            "a build links (blake3's and psm's assembly is their own, under their crate licence).",
            "OpenSSL 3 (Apache-2.0), and zlib where the build finds it, are linked dynamically",
            "from the image's Debian packages, whose copyright files cover them.",
        ] + out
    return out, missing


def license_of(name: str, version: str) -> str | None:
    for src in REGISTRY.glob("*"):
        manifest = src / f"{name}-{version}" / "Cargo.toml"
        if manifest.exists():
            text = manifest.read_text()
            m = re.search(r'^license\s*=\s*"([^"]+)"', text, re.M)
            if m:
                return m.group(1)
            if re.search(r"^license-file", text, re.M):
                return "see the crate's bundled licence file"
            return None
    return None


def main() -> int:
    crates = [(n, v) for n, v in crates_from_lock() if n != SELF]
    rows, missing = [], []
    for name, version in sorted(crates):
        lic = license_of(name, version)
        (rows if lic else missing).append((name, version, lic))

    counts = collections.Counter(lic for _, _, lic in rows)
    out = [
        "# Third-party notices",
        "",
        "`ddi` is Apache-2.0 (see `LICENSE` and `NOTICE`). A binary build statically links",
        "the crates below. They are reproduced here to satisfy the attribution clauses of",
        "their licences, which apply whenever the binary — rather than the source — is",
        "distributed.",
        "",
        f"Generated by `scripts/third-party-notices.py` from `Cargo.lock`: "
        f"{len(rows)} crates.",
        "",
        "## Licences in use",
        "",
        "| Licence | Crates |",
        "|---|---|",
    ]
    out += [f"| {lic} | {n} |" for lic, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]
    out += ["", "## Crates", "", "| Crate | Version | Licence |", "|---|---|---|"]
    out += [f"| {n} | {v} | {lic} |" for n, v, lic in rows]

    if missing:
        out += [
            "",
            "## Not resolved",
            "",
            "These were not vendored locally when this file was generated, so their licence",
            "could not be read. Run `cargo fetch` and regenerate.",
            "",
        ]
        out += [f"- {n} {v}" for n, v, _ in missing]

    out += [
        "",
        "Full licence texts are distributed with each crate in the cargo registry, and are",
        "available from the projects themselves. The common ones are reproduced in full at:",
        "",
        "- Apache-2.0: http://www.apache.org/licenses/LICENSE-2.0",
        "- MIT: https://opensource.org/licenses/MIT",
    ]

    c_sections, c_missing = c_library_sections(crates)
    out += c_sections
    if c_missing:
        out += ["", "C licence files not found locally (run `cargo fetch` and regenerate):", ""]
        out += [f"- {m}" for m in c_missing]
    out.append("")

    (ROOT / "THIRD_PARTY_NOTICES.md").write_text("\n".join(out))
    print(
        f"wrote THIRD_PARTY_NOTICES.md: {len(rows)} crates, {len(missing)} unresolved, "
        f"{len(c_missing)} C licence files missing"
    )
    return 1 if missing or c_missing else 0


if __name__ == "__main__":
    sys.exit(main())
