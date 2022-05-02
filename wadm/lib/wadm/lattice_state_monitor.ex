defmodule Wadm.LatticeStateMonitor do
  @decay_timer_ms 39_000

  require Logger
  alias Phoenix.PubSub
  alias LatticeObserver.Observed.Lattice
  use GenServer

  def start_link(lattice_id) do
    GenServer.start_link(__MODULE__, lattice_id, name: String.to_atom("state_#{lattice_id}"))
  end

  @impl true
  def init(lattice_id) do
    state = LatticeObserver.Observed.Lattice.new(lattice_id)
    PubSub.subscribe(Wadm.PubSub, "lattice:#{lattice_id}")
    :timer.send_interval(@decay_timer_ms, :decay_lattice)

    {:ok, state}
  end

  @impl true
  def handle_info({:cloud_event, cloud_event}, state) do
    Logger.debug("Received cloud event: #{inspect(cloud_event)}")
    {:noreply, state}
  end

  @impl true
  def handle_info(:decay_lattice, state) do
    Logger.debug("Decay tick")

    lattice =
      state
      |> Lattice.apply_event(
        LatticeObserver.CloudEvent.new_synthetic(%{}, "decay_ticked", "none")
      )

    {:noreply, lattice}
  end
end
