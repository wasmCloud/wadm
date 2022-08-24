FROM elixir:1.13.4-otp-25-alpine as builder

# Install deps
RUN apk add git bash

# Copy source code
COPY ./ ./

# Install mix tools
RUN mix local.rebar --force && \
  mix local.hex --force

# Compile and build release
RUN MIX_ENV=prod mix do deps.get, compile, release wadm

# Copy release to slim container for efficiency
FROM alpine:3.16
WORKDIR /opt/app
# Required for the erlang VM
RUN apk add ncurses libgcc libstdc++
COPY --from=builder _build/prod/rel/wadm /opt/app

CMD ["/opt/app/bin/wadm", "start"]
