defmodule Wadm.Deployments.DeploymentMonitor do
  @doc """
  One of these processes should be running to observe and reconcile
  an appspec+version instance
  """
  use GenServer
  require Logger

  defmodule State do
    defstruct [:spec, :prefix, :key]
  end

  @spec start_link(Map.t()) :: GenServer.on_start()
  def start_link(%{app_spec: _app_spec, lattice_prefix: _prefix, key: _key} = opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    Logger.debug(
      "Starting Deployment Monitor for deployment #{opts.app_spec.name} v#{opts.app_spec.version}"
    )

    # Make sure there's a running lattice observer for this prefix
    {:ok, _monitor} = Wadm.Observer.RootLatticeObserver.start_monitor(opts.lattice_prefix)

    Registry.register(Registry.DeploymentMonitorRegistry, opts.key, [])
    Registry.register(Registry.DeploymentsByLatticeRegistry, opts.lattice_prefix, [])

    {:ok,
     %State{
       spec: opts.app_spec,
       prefix: opts.lattice_prefix,
       key: opts.key
     }}
  end

  def is_reconciliation_ready?(pid) do
    GenServer.call(pid, :is_ready)
  end

  @impl true
  def handle_info({:handle_event, lattice, _event}, state) do
    Logger.debug("Performing processing check on #{state.spec.name} v#{state.spec.version}")

    # TODO
    #
    # * Run reconcile against the lattice struct (new_state)
    # * Determine what to do with the recommended actions
    # * Emit actions to distributed work dispatch
    # re: emitting actions to work dispatch - this again feels like it could be more elegantly
    # solved by making this process a producer/consumer in a GenStage flow

    _actions = Wadm.Reconciler.AppSpec.reconcile(state.spec, lattice)

    {:noreply, state}
  end

  @impl true
  def handle_call(:is_ready, _from, state) do
    # TODO
    # Based on the current state of this deployment monitor, respond whether
    # it's ready to accept a new lattice state for reconciliation
    #
    # This should be used to keep the deployment monitor from taking action
    # before its previously dispatched actions have a chance to work their
    # way through. At the very least, some minimal time elapsed needs to take
    # place between reconciliation dispatches
    {:reply, true, state}
  end
end
