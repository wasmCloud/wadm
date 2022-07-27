FROM elixir:1.13.3-slim as builder

# Install deps
RUN apt update && \
    apt install git -y

# Copy source code
COPY ./ ./

# Install mix tools
RUN mix local.rebar --force && \
  mix local.hex --force

# Compile and build release
RUN MIX_ENV=prod mix do deps.get, compile, release wadm

# Copy release to slim container for efficiency
FROM debian:bullseye-slim
WORKDIR /opt/app
COPY --from=builder _build/prod/rel/wadm /opt/app

CMD ["/opt/app/bin/wadm", "start"]