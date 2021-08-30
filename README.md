# Autonomous Lattice Controller
The Autonomous Lattice Controller (ALC) was created in response to [this RFC](https://github.com/wasmCloud/wasmcloud-otp/issues/177). When deployed into a lattice, it is capable of managing a set of application deployment specifications, monitoring the current state of an entire lattice, and issuing the appropriate lattice control commands required to close the gap between observed and desired state.

## Responsibilities
The ALC has a very small set of responsibilities, which actually contributes to its power. 

* Manage Application Specifications - Manage models consisting of _desired state_. This includes the creation and deletion and _rollback_ of models to previous versions. Application specifications are defined using the [Open Application Model](https://oam.dev/). For more information, see our [OAM README](./oam/README.md).
* Observe State - Observe the current, real-time state of a lattice
* Take Compensating Actions - When indicated, issue commands to the lattice control interface to bring about the changes necessary to make the desired and observed state match.

## API
Interacting with the ALC is done over NATS using the _lattice control interface_ connection on the root topic `wasmbus.alc.{prefix}` where `prefix` is the lattice namespace prefix. For more information on this API, please consult the [ALC Reference](https://wasmcloud.dev/reference/alc).

## References
* [Autonomous Agents](https://www.sciencedirect.com/topics/computer-science/autonomous-agent)
* [Autonomous Agent (Wikipedia)](https://en.wikipedia.org/wiki/Autonomous_agent)
* [Control Loop](https://en.wikipedia.org/wiki/Control_loop)
