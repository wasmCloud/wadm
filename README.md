<img align="right" src="./wadm.png" alt="wadm logo" style="width: 200px" />

# wasmCloud Application Deployment Manager (wadm)

The wasmCloud Application Deployment Manager (**wadm**) enables declarative wasmCloud applications.
It's responsible for managing a set of application deployment specifications, monitoring the current
state of an entire [lattice](https://wasmcloud.com/docs/reference/lattice/), and issuing the
appropriate lattice control commands required to close the gap between observed and desired state.
It is currently in an `alpha` release state and undergoing further rigor and testing approaching a
production-ready `0.4` version.

**Heads Up**

Wadm is still in alpha as we continue to tie up a few loose ends. Below is a list of known
issues/missing features to be aware of:

- Currently the API does not update with the status of each scaler and reconcile action
- The current scaling algorithm is a bit jittery and puts the "eventual" in eventual consistency. It
  will get to the right number of replicas, but may take a second. We've already identified a fix,
  but it is in the the wasmCloud host, so we'll need to update things there first
- Full e2e tests. There are integration tests, but not full e2e tests in place yet
- Full/updated documentation
- More examples!

## Using wadm

### Install

Ensure you have a proper [rust](https://www.rust-lang.org/tools/install) toolchain installed.

```
cargo install wash-cli --git https://github.com/wasmcloud/wash --branch feat/wadm_0.4_support --force
cargo install wadm --bin wadm --features cli --git https://github.com/wasmcloud/wadm --version v0.4.0-alpha.1 --force
```

You can deploy **wadm** by downloading the binary for your host operating system and architecture,
and then running it alongside your wasmCloud host. We recommend using **wash** to run wasmCloud and
NATS, and then running **wadm** afterwards connected to the same NATS connection.

### Setup

```
wash up -d   # Start NATS and wasmCloud in the background
wadm         # Start wadm
```

### Deploying an application

Take the following manifest and save it locally (you can also download this from
[echo.yaml](./oam/echo.yaml)):

```yaml
apiVersion: core.oam.dev/v1beta1
kind: Application
metadata:
  name: echo
  annotations:
    version: v0.0.1
    description: "This is my app"
spec:
  components:
    - name: echo
      type: actor
      properties:
        image: wasmcloud.azurecr.io/echo:0.3.7
      traits:
        - type: spreadscaler
          properties:
            replicas: 1
        - type: linkdef
          properties:
            target: httpserver
            values:
              address: 0.0.0.0:8080

    - name: httpserver
      type: capability
      properties:
        contract: wasmcloud:httpserver
        image: wasmcloud.azurecr.io/httpserver:0.17.0
      traits:
        - type: spreadscaler
          properties:
            replicas: 1
```

Then, use **wadm** to put the manifest and deploy it.

```
wash app put ./echo.yaml
wash app deploy echo
```

🎉 You've just launched your first application with **wadm**! Try `curl localhost:8080/wadm` and see
the response from the [echo](https://github.com/wasmCloud/examples/tree/main/actor/echo) WebAssembly
module.

When you're done, you can use **wadm** to undeploy the application.

```
wash app undeploy echo
```

### Modifying applications

**wadm** supports upgrading applications by `put`ting new versions of manifests and then `deploy`ing
them. Try changing the manifest you created above by updating the number of echo replicas.

```yaml
<<ELIDED>>
  name: echo
  annotations:
    version: v0.0.2 # Note the changed version
    description: "wasmCloud echo Example"
spec:
  components:
    - name: echo
      type: actor
      properties:
        image: wasmcloud.azurecr.io/echo:0.3.5
      traits:
        - type: spreadscaler
          properties:
            replicas: 10 # Let's run 10!
<<ELIDED>>
```

Then, simply deploy the new version:

```
wash app put ./echo.yaml
wash app deploy echo v0.0.2
```

If you navigate to the [wasmCloud dashboard](http://localhost:4000/), you'll see that you now have
10 instances of the echo actor.

_Documentation for configuring the spreadscaler to spread actors and providers across multiple hosts
in a lattice is forthcoming._

## Responsibilities

**wadm** has a very small set of responsibilities, which actually contributes to its power.

- **Manage Application Specifications** - Manage models consisting of _desired state_. This includes
  the creation and deletion and _rollback_ of models to previous versions. Application
  specifications are defined using the [Open Application Model](https://oam.dev/). For more
  information on wadm's specific OAM features, see our [OAM README](./oam/README.md).
- **Observe State** - Monitor wasmCloud [CloudEvents](https://cloudevents.io/) from all hosts in a
  lattice to build the current state.
- **Take Compensating Actions** - When indicated, issue commands to the [lattice control
  interface](https://github.com/wasmCloud/interfaces/tree/main/lattice-control) to bring about the
  changes necessary to make the desired and observed state match.

## 🚧 Advanced

In advanced use cases, **wadm** is also capable of:

- Monitoring multiple lattices
- Running multiple replicas to distribute load among multiple processes, or for a high-availability
  architecture.

🚧 The above functionality is somewhat tested, but not as rigorously as a single instance monitoring
a single lattice. Proceed with caution while we do further testing.

### API

Interacting with **wadm** is done over NATS on the root topic `wadm.api.{prefix}` where `prefix` is
the lattice namespace prefix. For more information on this API, please consult the [wadm
Reference](https://wasmcloud.dev/reference/wadm).

## References

The wasmCloud Application Deployment Manager (**wadm**) originally came from [the autonomous lattice
controller RFC](https://github.com/wasmCloud/wasmcloud-otp/issues/177) and then was reworked in
version `0.4` in response to [the tidying and productioning
RFC](https://github.com/wasmCloud/wadm/issues/40).

- [Autonomous Agents](https://www.sciencedirect.com/topics/computer-science/autonomous-agent)
- [Autonomous Agent (Wikipedia)](https://en.wikipedia.org/wiki/Autonomous_agent)
- [Control Loop](https://en.wikipedia.org/wiki/Control_loop)
- [Hashicorp Vault](https://www.vaultproject.io/)
