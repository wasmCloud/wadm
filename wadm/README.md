# wasmCloud Application Deployment Manager (wadm)

The core functional OTP application of the wasmCloud Application Deployment Manager. Any UI or additional tooling for this application should add this package as a dependency.

## Installation
At the moment this application isn't being bundled for release. It will shortly, but in the meantime you can check out the main branch and start the application via `iex -S mix` in the root directory. 

## Prerequisites
You will need the following on your machine in order to run wadm:

* NATS (currently using anonymous local authentication, but real auth will come soon)
* Elixir v1.13 and OTP 25

You will also want a version of `wash` that is new enough to support the `app` subset of commands for interacting with the wadm API.

## To Release
Ensure you have the Elixir/OTP prerequisite above, then run the following command to create a [mix release](https://hexdocs.pm/mix/1.13/Mix.Tasks.Release.html) of `wadm`:
```shell
MIX_ENV=prod mix do deps.get, compile, release wadm
```

If you'd prefer to create a mix release with `Docker`, you can do the following:
```shell
docker build -t wadm:latest .
```