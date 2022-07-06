defmodule Wadm.LatticeSupervisor do
  @moduledoc """
  The lattice supervisor is started as a child of the Horde dynamic supervisor. This
  means that there is one lattice supervisor _per cluster_, and each one of these
  supervisors also starts the following children:

  * Gnat connection supervisor
  * Gnat consumer supervisor (Lattice Event Listener)
  * Lattice State Monitor
  * Lattice Control Client (TODO)
  """
  require Logger
  use Supervisor

  @doc """
  Use this function to start a lattice supervisor rather than invoking start_link directly,
  as this function will properly handle working within the Horde cluster
  """
  def start_lattice_supervisor(lattice_id) do
    # Init (or reuse) lattice supervisor
    pid = get_process(lattice_id)

    if pid == nil do
      Horde.DynamicSupervisor.start_child(
        Wadm.HordeSupervisor,
        {Wadm.LatticeSupervisor, lattice_id}
      )
    else
      {:ok, pid}
    end
  end

  @doc """
  Starts an instance of the lattice supervisor. Do not use this function directly, instead
  use the start_lattice_supervisor function
  """
  def start_link(lattice_id) do
    case Supervisor.start_link(__MODULE__, lattice_id, name: via_tuple(lattice_id)) do
      {:ok, res} ->
        {:ok, res}

      {:error, {:already_started, pid}} ->
        Logger.debug("Already running lattice supervisor at #{inspect(pid)}")
        :ignore
    end
  end

  @doc """
  Called only when a new instance of this supervisor is being created
  """
  @impl true
  def init(supervised_lattice_id) do
    Logger.info("Starting lattice supervisor for #{supervised_lattice_id}")
    lattice_id = String.to_atom(supervised_lattice_id)

    # TODO
    # get NATS configuration and credentials from secret store based on
    # the lattice ID
    gnat_supervisor_settings = %{
      name: lattice_id,
      backoff_period: 2000,
      connection_settings: [
        %{host: "127.0.0.1", port: 4222}
      ]
    }

    children = [
      Supervisor.child_spec(
        {Gnat.ConnectionSupervisor, gnat_supervisor_settings},
        id: lattice_id
      ),
      Supervisor.child_spec(
        {Gnat.ConsumerSupervisor,
         %{
           connection_name: lattice_id,
           module: Wadm.LatticeEventListener,
           subscription_topics: [
             %{topic: "*.wasmbus.evt.*", queue_group: "wadmevtmon"},
             %{topic: "wasmbus.evt.*", queue_group: "wadmevtmon"}
           ]
         }},
        id: String.to_atom("evt_#{supervised_lattice_id}")
      ),
      Supervisor.child_spec(
        {Wadm.LatticeStateMonitor, supervised_lattice_id},
        id: String.to_atom("st_#{supervised_lattice_id}")
      )
    ]

    Supervisor.init(children, strategy: :one_for_one)
    # IO.inspect(e)
    # e
    # Supervisor.init([], strategy: :one_for_one)
  end

  def via_tuple(lattice_id), do: {:via, Horde.Registry, {Wadm.HordeRegistry, lattice_id}}

  def get_process(lattice_id) when is_binary(lattice_id) do
    case Horde.Registry.lookup(Wadm.HordeRegistry, lattice_id) do
      [{pid, _val}] -> pid
      [] -> nil
    end
  end
end
