defmodule Wadm.Deployments.CloudEvents do
  require Logger

  @evt_model_version_created "model_version_created"
  @evt_model_version_deleted "model_version_deleted"
  @evt_model_deployed "model_deployed"
  @evt_model_undeployed "model_undeployed"
  @evt_deployment_state_changed "deployment_state_changed"
  @evt_control_action_taken "control_action_taken"
  @evt_control_action_failed "control_action_failed"
  @evt_reconciliation_error_occurred "reconciliation_error_occurred"

  def new(data, event_type) do
    stamp = DateTime.utc_now() |> DateTime.to_iso8601()

    %{
      specversion: "1.0",
      time: stamp,
      type: "com.wasmcloud.wadm.#{event_type}",
      source: "wadm",
      datacontenttype: "application/json",
      id: UUID.uuid4(),
      data: data
    }
    |> Cloudevents.from_map!()
    |> Cloudevents.to_json()
  end

  def model_version_created(name, version, lattice) do
    %{
      name: name,
      version: version,
      lattice_id: lattice
    }
    |> new(@evt_model_version_created)
  end

  def model_version_deleted(name, version, lattice) do
    %{
      name: name,
      version: version,
      lattice_id: lattice
    }
    |> new(@evt_model_version_deleted)
  end

  def model_deployed(name, version, lattice) do
    %{
      name: name,
      version: version,
      lattice_id: lattice
    }
    |> new(@evt_model_deployed)
  end

  def model_undeployed(name, version, lattice) do
    %{
      name: name,
      version: version,
      lattice_id: lattice
    }
    |> new(@evt_model_undeployed)
  end

  def deployment_state_changed(name, version, lattice, state) do
    %{
      name: name,
      version: version,
      lattice_id: lattice,
      state: state
    }
    |> new(@evt_deployment_state_changed)
  end

  def control_action_taken(name, version, lattice, action_type, params \\ %{}) do
    %{
      name: name,
      version: version,
      lattice_id: lattice,
      action_type: action_type,
      params: params
    }
    |> new(@evt_control_action_taken)
  end

  def control_action_failed(name, version, lattice, action_type, message, params \\ %{}) do
    %{
      name: name,
      version: version,
      lattice_id: lattice,
      action_type: action_type,
      message: message,
      params: params
    }
    |> new(@evt_control_action_failed)
  end

  def reconciliation_error_occurred(name, version, lattice, message, params \\ %{}) do
    %{
      name: name,
      version: version,
      lattice_id: lattice,
      message: message,
      params: params
    }
    |> new(@evt_reconciliation_error_occurred)
  end

  def publish(evt) do
    if Process.whereis(:api_nats) != nil do
      Gnat.pub(:api_nats, "wadm.evt", evt)
    else
      Logger.warning("Tried to publish event, but the NATS API connection isn't alive")
    end
  end
end
