
<img align="right" src="./wadm.png" alt="wadm logo" style="width: 200px" />

# wasmCloud Application Deployment Manager (wadm)
The wasmCloud Application Deployment Manager (**wadm**) was created in response to [this RFC](https://github.com/wasmCloud/wasmcloud-otp/issues/177). When deployed into a lattice, it is capable of managing a set of application deployment specifications, monitoring the current state of an entire lattice, and issuing the appropriate lattice control commands required to close the gap between observed and desired state.

## Responsibilities
**wadm** has a very small set of responsibilities, which actually contributes to its power. 

* **Manage Application Specifications** - Manage models consisting of _desired state_. This includes the creation and deletion and _rollback_ of models to previous versions. Application specifications are defined using the [Open Application Model](https://oam.dev/). For more information on wadm's specific OAM features, see our [OAM README](./oam/README.md).
* **Observe State** - Observe the current, real-time state of a lattice (done via the [lattice-observer](https://github.com/wasmCloud/lattice-observer) library).
* **Take Compensating Actions** - When indicated, issue commands to the lattice control interface to bring about the changes necessary to make the desired and observed state match.

## API
Interacting with **wadm** is done over NATS on the root topic `wadm.{prefix}` where `prefix` is the lattice namespace prefix. For more information on this API, please consult the [wadm Reference](https://wasmcloud.dev/reference/wadm).

## References
* [Autonomous Agents](https://www.sciencedirect.com/topics/computer-science/autonomous-agent)
* [Autonomous Agent (Wikipedia)](https://en.wikipedia.org/wiki/Autonomous_agent)
* [Control Loop](https://en.wikipedia.org/wiki/Control_loop)
* [Hashicorp Vault](https://www.vaultproject.io/)

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

The credentials stored within a vault secret are pushed as a JSON payload containing the fields `jwt` and `nkey_seed`. When stored properly, you can use the `vault` command line to inspect the credentials as shown below:

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
nkey_seed    also bob
```
In the preceding example, the value of `WADM_VAULT_PATH_TEMPLATE` was `/secret/creds/%l`. The **%l** token will be replaced by the lattice ID, which, in the case of this example, is **default**. Note that while vault injects the path `data` into the path, you should _not_ include that in your template.

While it's worth remembering that user JWTs do not contain any secrets, the nkey seed obviously is a secret, and so it's a matter of pragmatism to keep both jwt and seed in the same place.

If you want a few lattices to share the same credentials and another set to use different credentials, simply duplicate the keys stored in vault as you see fit.

You cannot currently use different NATS URLs for different lattices.