defmodule Wadm.Api.ApiServer do
  @moduledoc """
  Exposes the API for interacting with wadm over the NATS subject wadm.api.>
  For the full detailed spec of which topics correspond to which operations, consult
  the reference at https://wasmcloud.dev/reference/wadm/api/
  """
  require Logger
  use Gnat.Server
  alias Wadm.Model.Store

  def request(%{topic: topic, body: body}) do
    topic
    |> String.split(".")
    # wadm
    |> List.delete_at(0)
    # api
    |> List.delete_at(0)
    |> List.to_tuple()
    |> handle_request(body)
  end

  defp handle_request({lattice_id, "model", "undeploy", model_name}, body) do
    with {:ok, _params} <- Jason.decode(body) do
      monitor = Wadm.Deployments.DeploymentMonitor.get_process(model_name, lattice_id)

      if monitor == nil do
        {:reply, success_result(%{})}
      else
        Logger.debug("Stopping deployment monitor: #{model_name}")
        case Horde.DynamicSupervisor.terminate_child(Wadm.HordeSupervisor, monitor) do
          :ok ->
            {:reply, success_result(%{})}

          e ->
            {:reply, fail_result("Failed to stop deployment monitor: #{inspect(e)}")}
        end
      end
    end
  end

  defp handle_request({lattice_id, "model", "deploy", model_name}, body) do
    with {:ok, version} <- Jason.decode(body),
         {:ok, model} <-
           Store.get_model_version(
             :model_store,
             model_name,
             vclean(version["version"]),
             lattice_id
           ) do
      monitor = Wadm.Deployments.DeploymentMonitor.get_process(model_name, lattice_id)

      if monitor == nil do
        start_deployment(model.vetted, lattice_id)
      else
        spec = Wadm.Deployments.DeploymentMonitor.get_spec(monitor)
        v = vclean(version["version"])
        running_v = spec.version

        if v != running_v do
          # wrong version is running, stop old monitor
          Horde.DynamicSupervisor.terminate_child(Wadm.HordeSupervisor, monitor)
          start_deployment(model.vetted, lattice_id)
        else
          # desired version is already running, idempotent "no-op"
          {:reply, success_result(%{acknowledged: true})}
        end
      end
    else
      {:error, e} ->
        {:reply, fail_result("Failed to deploy app spec: #{e}")}
    end
  end

  # Retrieves a list of models stored in the given lattice, indicating
  # the deployment status of that model within the lattice
  defp handle_request({lattice_id, "model", "list"}, _body) do
    with {:ok, models} <- Store.lattice_models(:model_store, lattice_id) do
      {:reply,
       success_result(
         models
         |> Enum.map(fn model ->
           %{
             name: model.name,
             version: model.version,
             description: model.description,
             deployment_status: get_deployment_status(model.name, lattice_id)
           }
         end)
       )}
    else
      e ->
        {:reply, fail_result("Failed to query models: #{inspect(e)}")}
    end
  end

  # Retrieves the detail model map (both raw and normalized) of the app spec
  # model. The version to retrieve is specified on the incoming JSON request
  # in the "version" field
  defp handle_request({lattice_id, "model", "get", model_name}, body) do
    with {:ok, version} <- Jason.decode(body),
         {:ok, model_map} <-
           Store.get_model_version(
             :model_store,
             model_name,
             vclean(version["version"]),
             lattice_id
           ) do
      {:reply, success_result(model_map)}
    else
      e ->
        {:reply, fail_result("Failed to query model: #{inspect(e)}")}
    end
  end

  # Puts a model spec version in the given lattice. If that model and version
  # already exist, this request will return an error
  defp handle_request({lattice_id, "model", "put"}, body) do
    with {:ok, raw} <- model_decode(body),
         {:ok, vetted} <- Wadm.Model.AppSpec.from_map(raw),
         :ok <- Wadm.Model.Validator.validate_appspec(vetted) do
      if Wadm.Model.Store.model_version_exists?(
           :model_store,
           vetted.name,
           vetted.version,
           lattice_id
         ) do
        {:reply,
         fail_result(
           "Model #{vetted.name} v#{vetted.version} already exists in lattice #{lattice_id}, operation rejected"
         )}
      else
        case Wadm.Model.Store.put_model_version(:model_store, body, vetted, lattice_id) do
          :ok ->
            {:reply,
             success_result(%{
               current_version: vetted.version,
               name: vetted.name
             })}

          {:error, e} ->
            {:reply, fail_result("Failed to store model version: #{e}")}
        end
      end
    else
      e ->
        {:reply, fail_result("Failed to put model spec: #{inspect(e)}")}
    end
  end

  # Queries the list of versions available for a given model in the given lattice
  # Versions will be returned in order of newest-first
  defp handle_request({lattice_id, "model", "versions", model_name}, _body) do
    with {:ok, versions} <- Wadm.Model.Store.model_versions(:model_store, model_name, lattice_id) do
      {:reply,
       success_result(
         versions
         |> Enum.map(fn v ->
           %{
             version: v.version,
             created: v.created,
             deployed: is_version_deployed?(model_name, v.version, lattice_id)
           }
         end)
       )}
    else
      e ->
        {:reply, fail_result("Failed to query model version history: #{inspect(e)}")}
    end
  end

  # Deletes a model from the store in a given lattice. Right now the
  # auto-undeploy feature is ignored, eventually we want to stop the
  # corresponding deployment monitor
  # TODO - rename the undeploy field to "destructive", to indicate whether
  # resources being managed by a deployment monitor should be purged.
  defp handle_request({lattice_id, "model", "del", model_name}, body) do
    with {:ok, request} <- Jason.decode(body),
         {:ok, version} <-
           Wadm.Model.Store.get_model_version(
             :model_store,
             model_name,
             vclean(request["version"]),
             lattice_id
           ) do
      # TODO: not yet used
      _undeploy = request["undeploy"]

      case Wadm.Model.Store.delete_model_version(
             :model_store,
             model_name,
             vclean(request["version"]),
             lattice_id
           ) do
        :ok ->
          {:reply, success_result(%{version: vclean(version)})}

        {:error, e} ->
          {:reply, fail_result("Failed to delete model version: #{e}")}
      end
    else
      e ->
        {:reply, fail_result("Failed to delete model version: #{inspect(e)}")}
    end
  end

  defp success_result(payload) do
    %{
      result: "success",
      data: payload
    }
    |> Jason.encode!()
  end

  defp fail_result(msg) do
    %{
      result: "error",
      message: msg
    }
    |> Jason.encode!()
  end

  defp get_deployment_status(model_name, lattice_id) do
    monitor = Wadm.Deployments.DeploymentMonitor.get_process(model_name, lattice_id)

    if monitor == nil do
      :undeployed
    else
      Wadm.Deployments.DeploymentMonitor.get_deployment_status(monitor)
    end
  end

  defp vclean(nil), do: ""
  defp vclean("v" <> v), do: v
  defp vclean(v), do: v

  defp model_decode("{" <> _rest = body), do: Jason.decode(body)
  defp model_decode(body), do: YamlElixir.read_from_string(body)

  defp start_deployment(model, lattice_id) do
    case Wadm.Deployments.DeploymentMonitor.start_deployment_monitor(model, lattice_id) do
      {:ok, _pid} ->
        {:reply, success_result(%{acknowledged: true})}

      {:error, e} ->
        {:reply, fail_result("Failed to start deployment monitor: #{e}")}
    end
  end

  # Helper function that determines if a model version is deployed. A model
  # is deployed if there is a running deployment monitor for that model+lattice
  # and the version inside the encapsulated spec/state matches the target.
  defp is_version_deployed?(model_name, version, lattice_id) do
    pid = Wadm.Deployments.DeploymentMonitor.get_process(model_name, lattice_id)

    if pid == nil do
      false
    else
      spec = Wadm.Deployments.DeploymentMonitor.get_spec(pid)

      if spec != nil do
        spec.version == version
      else
        false
      end
    end
  end
end
