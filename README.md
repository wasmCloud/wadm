![wadm logo](./wadm.png)
# wasmCloud Application Deployment Manager (wadm) ![deploy jenkins](./deployjenkins.png)

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
* Mascot logo at the top of this page is _"Deeeeploy Jeeennnkinnns"_, an avatar used in a game demo for early versions of wasmCloud.