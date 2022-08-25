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

## Secrets and Multitenancy
The wasmCloud Application Deployment Manager (`wadm`) is an inherently multitenant system. It is designed to support managing multiple application deployments across multiple lattices. When running wadm locally or in simple scenarios you may be reusing the same NATS connection for all lattices, but that may not always be the case.

### Integrating with Vault
In a production-grade multitenant environment, each of the lattices that need to be observed by wadm may belong to different NATS users and/or accounts. As a result, wadm can look up the credentials for any given lattice within a Hashicorp Vault.

To enable vault lookups, supply a vault token via the `WADM_VAULT_TOKEN` environment variable. If this variable is empty or missing, wadm will use its default behavior of reusing the same NATS connection for all lattices.

The following environment variables are used for wadm's vault integration:

| Variable | Default Value | Description |
| --- | --- | --- |
| `WADM_VAULT_TOKEN` | `(Empty)` | A token used to authenticate against a vault instance |
| `WADM_VAULT_ADDR` | `http://127.0.0.1:8200` | The address of the vault instance |
| `WADM_VAULT_PATH_TEMPLATE` | `(Empty)` | A template used to define the vault path for lattice user credentials |

The credentials stored within a vault secret are pushed as a JSON payload containing the fields `jwt` and `seed`. When stored properly, you can use the `vault` command line to inspect the credentials as shown below:

```
$ vault kv get /secret/creds/default
====== Secret Path ======
secret/data/creds/default

======= Metadata =======
Key                Value
---                -----
created_time       2022-08-03T14:58:57.102257597Z
custom_metadata    <nil>
deletion_time      n/a
destroyed          false
version            1

====== Data ======
Key          Value
---          -----
jwt          bob
seed         also bob
```
In the preceding example, the value of `WADM_VAULT_PATH_TEMPLATE` was `/secret/creds/%l`. The **%l** token will be replaced by the lattice ID, which, in the case of this example, is **default**. Note that while vault injects the path `data` into the path, you should _not_ include that in your template.

While it's worth remembering that user JWTs do not contain any secrets, the nkey seed obviously is a secret, and so it's a matter of pragmatism to keep both jwt and seed in the same place.

If you want a few lattices to share the same credentials and another set to use different credentials, simply duplicate the keys stored in vault as you see fit.

You cannot currently use different NATS URLs for different lattices.

## To Release
Ensure you have the Elixir/OTP prerequisite above, then run the following command to create a [mix release](https://hexdocs.pm/mix/1.13/Mix.Tasks.Release.html) of `wadm`:
```shell
MIX_ENV=prod mix do deps.get, compile, release wadm
```

If you'd prefer to create a mix release with `Docker`, you can do the following:
```shell
docker build -t wadm:latest .
```