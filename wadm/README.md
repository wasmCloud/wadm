# wasmCloud Application Deployment Manager (wadm)

The core functional OTP application of the wasmCloud Application Deployment Manager. Any UI or additional tooling for this application should add this package as a dependency.

## Running wadm
Simply download the latest [release](https://github.com/wasmCloud/wadm/releases) of wadm based on your architecture and operating system, unpack, and run. We currently package `wadm` for x86_64 architectures on Linux, Mac, and Windows, and the following instructions are the same for all (simply replace the OS with your own):

### From Release:
```
wget https://github.com/wasmCloud/wadm/releases/download/v0.2.0/x86_64-linux.tar.gz
mkdir -p wadm
tar -xvf x86_64-linux.tar.gz -C wadm
cd wadm
./bin/wadm start
```
### With Docker Compose:
You can see an example compose configuration in the [petclinic docker-compose](https://github.com/wasmCloud/examples/blob/main/petclinic/docker/docker-compose.yml#L111) file, and you can run it with:
```
wget https://raw.githubusercontent.com/wasmCloud/examples/main/petclinic/docker/docker-compose.yml
docker compose up
```

## Installing from Source
You will need the following on your machine in order to run wadm:

* NATS (currently using anonymous local authentication, but real auth will come soon)
* Elixir v1.13 and OTP 25

You will also want a version of `wash` that is new enough to support the `app` subset of commands for interacting with the wadm API.

Once you have the above, simply run the following to install dependencies, compile, and run `wadm`
```
cd wadm/wadm
mix do deps.get, compile
iex -S mix
```


## To Release
Ensure you have the Elixir/OTP prerequisite above, then run the following command to create a [mix release](https://hexdocs.pm/mix/1.13/Mix.Tasks.Release.html) of `wadm`:
```shell
MIX_ENV=prod mix do deps.get, compile, release wadm
```

If you'd prefer to create a mix release with `Docker`, you can do the following:
```shell
docker build -t wadm:latest .
```