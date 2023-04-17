FROM rust:1.68.0-slim as builder

WORKDIR /usr/src/wadm

COPY . /usr/src/wadm/

RUN rustup target add x86_64-unknown-linux-musl
RUN apt update && apt install -y musl-tools musl-dev
RUN update-ca-certificates

RUN cargo build --bin wadm --features cli --target x86_64-unknown-linux-musl --release

FROM alpine:3.16.0 AS runtime 

ARG USERNAME=wadm
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN addgroup -g $USER_GID $USERNAME \
    && adduser -D -u $USER_UID -G $USERNAME $USERNAME

# Copy application binary from builder image
COPY --from=builder --chown=$USERNAME /usr/src/wadm/target/x86_64-unknown-linux-musl/release/wadm /usr/local/bin/wadm

USER $USERNAME
# Run the application
ENTRYPOINT ["/usr/local/bin/wadm"]
