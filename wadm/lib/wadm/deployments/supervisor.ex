defmodule Wadm.Deployments.Supervisor do
  use DynamicSupervisor
  require Logger

  alias Wadm.Model.AppSpec

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    Process.flag(:trap_exit, true)

    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @spec start_deployment_monitor(AppSpec.t(), String.t()) :: {:error, any} | {:ok, pid()}
  def start_deployment_monitor(app_spec, lattice_prefix) do
    case find_monitor(app_spec.name, app_spec.version, lattice_prefix) do
      nil ->
        DynamicSupervisor.start_child(
          __MODULE__,
          {Wadm.Deployments.DeploymentMonitor,
           %{
             app_spec: app_spec,
             lattice_prefix: lattice_prefix,
             key: child_key(app_spec.name, app_spec.version, lattice_prefix)
           }}
        )

      pid ->
        {:ok, pid}
    end
  end

  defp child_key(spec_name, version, prefix) do
    "#{spec_name}-#{version}-#{prefix}" |> Base.url_encode64()
  end

  def find_monitor(spec_name, version, lattice_prefix) do
    case Registry.lookup(
           Registry.DeploymentMonitorRegistry,
           child_key(spec_name, version, lattice_prefix)
         ) do
      [{mpid, _}] -> mpid
      _ -> nil
    end
  end
end
