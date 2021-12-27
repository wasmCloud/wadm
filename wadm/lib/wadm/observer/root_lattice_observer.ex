defmodule Wadm.Observer.RootLatticeObserver do
  use DynamicSupervisor
  require Logger

  def start_link(_arg) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @spec start_monitor(binary()) :: {:ok, pid} | :ignore | {:error, any}
  def start_monitor(lattice_prefix) when is_binary(lattice_prefix) do
    case find_monitor(lattice_prefix) do
      nil ->
        Logger.debug("Starting new lattice observer for prefix '#{lattice_prefix}'")

        DynamicSupervisor.start_child(
          __MODULE__,
          {LatticeObserver.NatsObserver,
           %{
             supervised_connection: :gnats_connection_supervisor,
             module: Wadm.Observer,
             lattice_prefix: lattice_prefix
           }}
        )

      pid ->
        Logger.debug("Reusing existing lattice observer for prefix '#{lattice_prefix}'")
        {:ok, pid}
    end
  end

  def find_monitor(prefix) do
    # Note this registry is maintained by the "lattice observer" library
    case Registry.lookup(Registry.LatticeObserverRegistry, prefix) do
      [{mpid, _}] -> mpid
      _ -> nil
    end
  end
end
