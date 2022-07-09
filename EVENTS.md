# wasmCloud Application Deployment Manager - Events
**wadm** emits all events on the `wadm.evt` subject in the form of [CloudEvents]().

The following is a list of the events emitted by wadm and the field names of the payload
carried within the cloud event's `data` field (stored as JSON). Each of the following events are in the `com.wasmcloud.wadm` namespace, so the event type for `model_version_created` is actually `com.wasmcloud.wadm.model_version_created`

| Event Type | Fields | Description |
| --- | --- | --- |
| `model_version_created` | name, version, lattice_id | Indicates that a new version of a model has been stored |
| `model_version_deleted` | name, version, lattice_id | Indicates that a specific model version has been deleted |
| `model_deployed` | name, version, lattice_id | Indicates that a deployment monitor process has started (and nothing more) |
| `model_undeployed` | name, version, lattice_id | Indicates that a deployment monitor process has been stopped (and nothing more) |
| `deployment_state_changed` | name, version, lattice_id, state | Indicates that a deployment monitor has changed state |
| `control_action_taken` | name, version, lattice_id, action_type, params(map) | Indicates that a deployment monitor has taken corrective action as a result of reconciliation |
| `control_action_failed` | name, version, lattice_id, action_type, message, params(map)  | Indicates a failure to submit corrective action to a lattice control API |
| `reconciliation_error_occurred` | name, version, lattice_id, message, params(map) | Indicates that required corrective action as a result of reconciliation cannot be performed (e.g. insufficient resources) |