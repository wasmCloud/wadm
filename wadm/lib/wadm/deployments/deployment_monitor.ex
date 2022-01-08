defmodule Wadm.Deployments.DeploymentMonitor do
  @doc """
  One of these processes should be running to observe and reconcile
  an appspec+version instance.

  This is a Horde-managed server and so only one of these will be running within
  the horde cluster at any given time, and Horde can relocate this to any cluster
  member as it sees fit. Make sure you only ever provision this with the start_deployment_monitor
  function.

  Process Drift
  When a node in a wadm cluster shuts down (intentionally or otherwise), this process
  will be re-started on a new target node _without saving state_. An instant later, that
  same process will try and reconstitute its state from cache.
  """
  use GenServer
  require Logger
  alias LatticeObserver.Observed.Lattice

  @decay_timer_ms 39_000

  @configuredcache [Wadm.Observer.RedisCache]

  defmodule State do
    defstruct [:lattice, :spec, :prefix, :key]
  end

  @spec start_link(Map.t()) :: GenServer.on_start()
  def start_link(%{app_spec: _app_spec, lattice_prefix: _prefix, key: key} = opts) do
    case GenServer.start_link(__MODULE__, opts, name: via_tuple(key)) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        Logger.debug("Already running deployment monitor at #{inspect(pid)}")
        :ignore

      other ->
        other
    end
  end

  @impl true
  def init(opts) do
    Logger.debug(
      "Starting Deployment Monitor for deployment #{opts.app_spec.name} v#{opts.app_spec.version}"
    )

    Process.send_after(self(), :decay_lattice, @decay_timer_ms)

    {:ok,
     %State{
       spec: opts.app_spec,
       prefix: opts.lattice_prefix,
       key: opts.key,
       lattice: Lattice.new(opts.lattice_prefix)
     }, {:continue, :check_cache}}
  end

  @impl true
  def handle_continue(:check_cache, state) do
    lattice = Enum.map(@configuredcache, fn cache -> cache.load_lattice(state.prefix) end)
    # lattice = configuredcache.load_lattice(state.prefix)

    if lattice != nil do
      Logger.debug("Reloading lattice state from cache")
      {:noreply, %{state | lattice: lattice}}
    else
      {:noreply, state}
    end
  end

  def lattice_prefix(pid) do
    GenServer.call(pid, :lattice_prefix)
  end

  @impl true
  def handle_call(:lattice_prefix, _from, state) do
    {:reply, state.prefix, state}
  end

  @impl true
  def handle_info(:decay_lattice, state) do
    Logger.debug("Decay tick")

    lattice =
      state.lattice
      |> Lattice.apply_event(
        LatticeObserver.CloudEvent.new_synthetic(%{}, "decay_ticked", "none")
      )

    if lattice != state.lattice do
      Enum.map(@configuredcache, fn cache -> cache.write_lattice(lattice) end)

      # configuredcache.write_lattice(lattice)
    end

    state = %{
      state
      | lattice: lattice
    }

    Process.send_after(self(), :decay_lattice, @decay_timer_ms)

    {:noreply, state}
  end

  @impl true
  def handle_info({:handle_event, event}, state) do
    Logger.debug(
      "Performing processing check on #{state.spec.name} v#{state.spec.version} (#{event.type})"
    )

    # TODO
    #
    # * Determine what to do with the recommended actions
    # * Emit actions to distributed work dispatch

    lattice = state.lattice |> Lattice.apply_event(event)

    if lattice != state.lattice do
      Enum.map(@configuredcache, fn cache -> cache.write_lattice(lattice) end)

      # configuredcache.write_lattice(lattice)
    end

    state = %{state | lattice: lattice}

    _actions = Wadm.Reconciler.AppSpec.reconcile(state.spec, lattice)

    {:noreply, state}
  end

  @spec start_deployment_monitor(AppSpec.t(), String.t()) :: {:error, any} | {:ok, pid()}
  def start_deployment_monitor(app_spec, lattice_prefix) do
    opts = %{
      app_spec: app_spec,
      lattice_prefix: lattice_prefix,
      key: child_key(app_spec.name, app_spec.version, lattice_prefix)
    }

    # Init (or reuse) deployment monitor
    Horde.DynamicSupervisor.start_child(
      Wadm.HordeSupervisor,
      {Wadm.Deployments.DeploymentMonitor, opts}
    )

    # Init (or reuse) lattice monitor
    Horde.DynamicSupervisor.start_child(
      Wadm.HordeSupervisor,
      {Wadm.Deployments.LatticeMonitor, opts}
    )
  end

  defp child_key(spec_name, version, prefix) do
    "#{spec_name}-#{version}-#{prefix}" |> Base.url_encode64()
  end

  # Within a libcluster-formed BEAM cluster, each deployment process is
  # uniquely identified by the hash of its spec name, spec version, and
  # the lattice ID with which it interacts
  def via_tuple(name), do: {:via, Horde.Registry, {Wadm.HordeRegistry, name}}
end
