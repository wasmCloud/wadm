defmodule Wadm.Observer.MnesiaCache do
  require Logger
  @behaviour Wadm.Observer.Cache
  @impl true
  def load_lattice(prefix) when is_binary(prefix) do
    Logger.debug("Fetching observed lattice state ('#{prefix}') from cache")
    key = "wadmcache:#{prefix}"

    :mnesia.transaction(fn -> :mnesia.read({Wadm.LatticeStore, key}) end)
  end

  # Writes an observed lattice to distributed cache
  @impl true
  def write_lattice(lattice = %LatticeObserver.Observed.Lattice{}) do
    key = "wadmcache:#{lattice.id}"
    value = :erlang.term_to_binary(lattice)

    :mnesia.transaction(fn -> :mnesia.write({Wadm.LatticeStore, "id", key, value}) end)
  end
end
