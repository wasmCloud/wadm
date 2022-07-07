defmodule Wadm.LatticeStateMonitor do
  @decay_timer_ms 39_000

  require Logger
  alias Phoenix.PubSub
  alias LatticeObserver.Observed.Lattice
  use GenServer

  def start_link(lattice_id) do
    GenServer.start_link(__MODULE__, lattice_id, name: via_tuple(lattice_id))
  end

  def get_state(pid) do
    GenServer.call(pid, :get_state)
  end

  @impl true
  def init(lattice_id) do
    Logger.debug("Initializing lattice state monitor for lattice #{lattice_id}")
    state = LatticeObserver.Observed.Lattice.new(lattice_id)
    PubSub.subscribe(Wadm.PubSub, "lattice:#{lattice_id}")
    :timer.send_interval(@decay_timer_ms, :decay_lattice)

    {:ok, state}
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_info({:cloud_event, cloud_event}, state) do
    Logger.debug("Received cloud event: #{cloud_event.type}")

    nlattice = state |> Lattice.apply_event(cloud_event)

    if state_changed?(nlattice, state) do
      dispatch_lattice_changed(nlattice, cloud_event)
    end

    {:noreply, nlattice}
  end

  @impl true
  def handle_info(:decay_lattice, state) do
    Logger.debug("Decay tick")

    event = LatticeObserver.CloudEvent.new_synthetic(%{}, "decay_ticked", "none")

    nlattice =
      state
      |> Lattice.apply_event(event)

    if state_changed?(nlattice, state) do
      dispatch_lattice_changed(nlattice, event)
    end

    {:noreply, nlattice}
  end

  # Determine if the old and new states are _meaningfully_ different. We can't
  # use the root lattice because that maintains "last seen" timestamps on hosts, etc
  defp state_changed?(new_lattice, old_lattice) do
    old_hosts = old_lattice.hosts |> Map.keys() |> Enum.sort()
    new_hosts = new_lattice.hosts |> Map.keys() |> Enum.sort()

    new_lattice.actors != old_lattice.actors ||
      new_lattice.providers != old_lattice.providers ||
      new_lattice.linkdefs != old_lattice.linkdefs ||
      old_hosts != new_hosts
  end

  defp dispatch_lattice_changed(lattice, event) do
    Logger.debug("Dispatching lattice changed (performing reconcile)")
    PubSub.broadcast(Wadm.PubSub, "deployments:#{lattice.id}", {:lattice_changed, lattice, event})
  end

  def via_tuple(lattice_id), do: {:via, Horde.Registry, {Wadm.HordeRegistry, "st_#{lattice_id}"}}

  def get_process(lattice_id) when is_binary(lattice_id) do
    case Horde.Registry.lookup(Wadm.HordeRegistry, "st_#{lattice_id}") do
      [{pid, _val}] -> pid
      [] -> nil
    end
  end
end
