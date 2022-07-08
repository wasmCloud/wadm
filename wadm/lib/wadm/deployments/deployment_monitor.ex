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
  will be re-started on a new target node. If the lattice monitor for the deployment stayed
  up, then it will only take the process a few seconds to reorient itself. If the monitor also died,
  then the resurrection of this process will start a new monitor, which will in turn reprobe the
  lattice to catch up.
  """
  use GenServer
  require Logger
  alias Phoenix.PubSub

  # Currently using a naive state model. Obvious room for iterative improvement
  # process starts in initializing
  # after 45s, if the process is still in "initializing" state (no lattice changed events received)
  # grab the current state and do a reconcile
  #
  # the main post-initializing loop should alternate between idle and reconciling
  # (no error state modeled yet)
  @type reconstate() :: :idle | :reconciling | :initializing

  defmodule State do
    @type t() :: %__MODULE__{
            spec: Map.t(),
            lattice_id: String.t(),
            recon_state: Wadm.Deployments.DeploymentMonitor.reconstate()
          }

    defstruct [:spec, :lattice_id, :recon_state]
  end

  @spec start_link(Map.t()) :: GenServer.on_start()
  def start_link(%{app_spec: app_spec, lattice_id: lattice_id} = opts) do
    case GenServer.start_link(__MODULE__, opts, name: via_tuple(app_spec.name, lattice_id)) do
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

    PubSub.subscribe(Wadm.PubSub, "deployments:#{opts.lattice_id}")

    {:ok,
     %State{
       spec: opts.app_spec,
       lattice_id: opts.lattice_id,
       recon_state: :initializing
     }, {:continue, :ensure_lattice_supervisor}}
  end

  @impl true
  def handle_call(:get_spec, _from, state), do: {:reply, state.spec, state}

  @impl true
  def handle_call(:get_recon_status, _from, state) do
    {:reply, state.recon_state, state}
  end

  @impl true
  def handle_continue(:ensure_lattice_supervisor, state) do
    # Make sure that there's a lattice supervisor running
    {:ok, _pid} = Wadm.LatticeSupervisor.start_lattice_supervisor(state.lattice_id)

    Process.send_after(self(), :reconcile_initializing, 45_000)

    {:noreply, state}
  end

  @impl true
  def handle_info(:remove_backoff, state) do
    Wadm.Deployments.CloudEvents.deployment_state_changed(
      state.spec.name,
      state.spec.version,
      state.lattice_id,
      "idle"
    )
    |> Wadm.Deployments.CloudEvents.publish()

    {:noreply, %State{state | recon_state: :idle}}
  end

  @impl true
  def handle_info(:reconcile_initializing, state) do
    # TODO - deal with error state
    if state.recon_state == :initializing do
      pid = Wadm.LatticeStateMonitor.get_process(state.lattice_id)

      if pid != nil do
        {cmd_count, error_count} = do_reconcile(state.spec, state.lattice_id)

        # TODO - when we have a discrete error state we should enumerate each of
        # these reconciliation failures and make them available as part of the
        # state for querying/event emission.
        if error_count > 0 do
          Wadm.Deployments.CloudEvents.reconciliation_error_occurred(
            state.spec.name,
            state.spec.version,
            state.lattice_id,
            "#{error_count} errors occurred during reconciliation pass"
          )
          |> Wadm.Deployments.CloudEvents.publish()
        end

        if cmd_count > 0 do
          Wadm.Deployments.CloudEvents.deployment_state_changed(
            state.spec.name,
            state.spec.version,
            state.lattice_id,
            "reconciling"
          )
          |> Wadm.Deployments.CloudEvents.publish()

          {:noreply, %State{state | recon_state: :reconciling}}
        else
          Wadm.Deployments.CloudEvents.deployment_state_changed(
            state.spec.name,
            state.spec.version,
            state.lattice_id,
            "idle"
          )
          |> Wadm.Deployments.CloudEvents.publish()

          {:noreply, %State{state | recon_state: :idle}}
        end
      else
        Wadm.Deployments.CloudEvents.deployment_state_changed(
          state.spec.name,
          state.spec.version,
          state.lattice_id,
          "idle"
        )
        |> Wadm.Deployments.CloudEvents.publish()

        {:noreply, %State{state | recon_state: :idle}}
      end
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:lattice_changed, lattice, _event}, state) do
    # in backoff/cooldown mode, incoming events have no effect
    if state.recon_state == :reconciling do
      {:noreply, state}
    else
      Logger.debug(
        "Deployment monitor #{state.spec.name} handling state change for lattice #{lattice.id}"
      )

      # TODO make use of the error state
      _ = do_reconcile(state.spec, lattice)

      {:noreply, %State{state | recon_state: :reconciling}}
    end
  end

  defp do_reconcile(spec, lattice) do
    {skips, cmds} =
      Wadm.Reconciler.AppSpec.reconcile(spec, lattice)
      |> Enum.split_with(fn command -> command.cmd == :no_action || command.cmd == :error end)

    errors = skips |> Enum.filter(fn command -> command.cmd == :error end)

    if length(errors) > 0 do
      # TODO - once we move past the naive reconciliation strategy, we might want a
      # discrete error/failed state
      Logger.error("Failed to perform reconciliation pass: #{errors |> Enum.join(",")}")
    end

    Logger.debug("Reconciliation found #{length(skips)} no-ops and #{length(cmds)} commands")

    cmds
    |> Enum.map(fn cmd ->
      {spec, cmd, Wadm.Reconciler.Command.to_lattice_control_command(spec, lattice.id, cmd),
       lattice.id}
    end)
    |> Enum.each(&publish_lattice_control_command/1)

    # even if we had a failure in the command list, back off so that we don't attempt
    # to re-reconcile for another 45 seconds
    Process.send_after(self(), :remove_backoff, 45_000)

    {length(cmds), length(errors)}
  end

  defp publish_lattice_control_command({spec, orig_command, {topic, cmd}, lattice_id}) do
    # TODO - include the command data in the params field of these event pubs
    case Gnat.request(String.to_atom(lattice_id), topic, cmd) do
      {:ok, %{body: res}} ->
        case Jason.decode(res) do
          {:ok, %{"accepted" => false, "error" => err}} ->
            Wadm.Deployments.CloudEvents.control_action_failed(
              spec.name,
              spec.version,
              lattice_id,
              "#{orig_command.cmd}",
              err
            )
            |> Wadm.Deployments.CloudEvents.publish()

            Logger.error("Lattice control interface rejected request: #{err}")

          _ ->
            Wadm.Deployments.CloudEvents.control_action_taken(
              spec.name,
              spec.version,
              lattice_id,
              "#{orig_command.cmd}"
            )
            |> Wadm.Deployments.CloudEvents.publish()

            Logger.debug("Successfully performed lattice control interface request")
        end

      {:error, _} ->
        Wadm.Deployments.CloudEvents.control_action_failed(
          spec.name,
          spec.version,
          lattice_id,
          "#{orig_command.cmd}",
          "NATS API request timeout"
        )
        |> Wadm.Deployments.CloudEvents.publish()

        Logger.error("Timeout occurred attempting to make lattice control interface request")
    end
  end

  @spec start_deployment_monitor(AppSpec.t(), String.t()) :: {:error, any} | {:ok, pid()}
  def start_deployment_monitor(app_spec, lattice_id) do
    opts = %{
      app_spec: app_spec,
      lattice_id: lattice_id
    }

    pid = get_process(opts.app_spec.name, opts.lattice_id)

    if pid == nil do
      Horde.DynamicSupervisor.start_child(
        Wadm.HordeSupervisor,
        {Wadm.Deployments.DeploymentMonitor, opts}
      )
    else
      {:ok, pid}
    end
  end

  # Within a libcluster-formed BEAM cluster, each deployment manager is
  # uniquely identified by its spec name and the lattice in which it's
  # running
  def via_tuple(spec_name, lattice_id),
    do: {:via, Horde.Registry, {Wadm.HordeRegistry, "depmon_#{spec_name}_#{lattice_id}"}}

  def get_process(spec_name, lattice_id) when is_binary(lattice_id) do
    case Horde.Registry.lookup(Wadm.HordeRegistry, "depmon_#{spec_name}_#{lattice_id}") do
      [{pid, _val}] -> pid
      [] -> nil
    end
  end

  def get_deployment_status(pid) do
    GenServer.call(pid, :get_recon_status)
  end

  def get_spec(pid) do
    if Process.alive?(pid) do
      GenServer.call(pid, :get_spec)
    else
      nil
    end
  end
end
