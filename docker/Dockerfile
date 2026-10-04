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
# name it the same way on purpose: whoever edits one line can see the other. The same rule
# covers OpenSSL: a `kafka` build links the build stage's libssl.so.3 dynamically, and the
# runtime stage's libssl3 must be that library's ABI, which the same Debian release is.

# Build
FROM rust:1.97-bookworm AS build
# Cargo features to build with: empty is the Delta-only binary; `kafka` adds the Kafka source
# (docs/kafka-design.md), which stays empty here until that source is complete (M3).
#   docker build --build-arg DDI_FEATURES=kafka -f docker/Dockerfile .
ARG DDI_FEATURES=
# pkg-config and libssl-dev: OpenSSL 3 for librdkafka's TLS and SASL/SCRAM. zlib1g-dev: its
# gzip codec, linked dynamically. make: librdkafka's own build. libclang-dev: the zstd-sys
# this tree locks (2.0.16) turns on bindgen by default, and rdkafka-sys asks for its defaults.
# All but libclang-dev already come with the rust image; they are named so a base-image change
# cannot remove them silently. Only a `kafka` build uses any of them.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev zlib1g-dev make libclang-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# Cache the dependency build across source changes. Both builds take the same features, or
# the second one rebuilds every dependency the first one cached.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs && echo '' > src/lib.rs \
    && cargo build --release --locked --features "$DDI_FEATURES" && rm -rf src
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked --features "$DDI_FEATURES"

# Runtime — bookworm, matching the build stage above.
# libssl3 is named because a `kafka` build links it, and until now it only arrived as a
# dependency of ca-certificates' openssl, which is nothing to rely on. zlib1g is part of every
# Debian base.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 ddi
COPY --from=build /src/target/release/ddi /usr/local/bin/ddi
USER ddi

# Prove the binary starts, in the image that will actually run it and as the user that will
# run it. The pin above fixes today; this fixes the next base-image bump, because a binary
# the loader cannot resolve now fails the build that produced it instead of the deployment
# that used it. `--version` is enough: the loader resolves every shared library before
# `main` runs, so a mismatch cannot get this far. A `kafka` build also prints the librdkafka
# it linked, which is how an image says whether it can read Kafka.
RUN /usr/local/bin/ddi --version

ENV RUST_LOG=info,deltalake=warn,deltalake_core=warn,buoyant_kernel=warn,datafusion=warn
ENTRYPOINT ["/usr/local/bin/ddi"]
CMD ["run"]
