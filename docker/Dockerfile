# The two Debian releases below MUST stay equal.
#
# A Rust binary links against the glibc of the image it was built in, and glibc symbols are
# versioned: a binary built against 2.41 asks the loader for `GLIBC_2.38`/`GLIBC_2.39`
# symbols that a 2.36 runtime does not have. It then fails to start at all — before `main`,
# so no amount of care inside the program helps.
#
# What makes this worth a comment rather than a convention: `docker build` never executes
# what it copies, so the build stays green and the image is published broken. The failure
# surfaces in the first job that runs the container, which may be days later and looks like
# a deployment problem rather than a build one.
#
# `rust:1.97-slim` is not a pin — the unsuffixed variants track the newest Debian, which
# moved to trixie (glibc 2.41). Both stages therefore name `bookworm` explicitly, and they
# name it the same way on purpose: whoever edits one line can see the other.

# Build
FROM rust:1.97-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# Cache the dependency build across source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs && echo '' > src/lib.rs \
    && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

# Runtime — bookworm, matching the build stage above.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 ddi
COPY --from=build /src/target/release/ddi /usr/local/bin/ddi
USER ddi

# Prove the binary starts, in the image that will actually run it and as the user that will
# run it. The pin above fixes today; this fixes the next base-image bump, because a binary
# the loader cannot resolve now fails the build that produced it instead of the deployment
# that used it. `--version` is enough: the loader resolves every shared library before
# `main` runs, so a mismatch cannot get this far.
RUN /usr/local/bin/ddi --version

ENV RUST_LOG=info,deltalake=warn,deltalake_core=warn,buoyant_kernel=warn,datafusion=warn
ENTRYPOINT ["/usr/local/bin/ddi"]
CMD ["run"]
