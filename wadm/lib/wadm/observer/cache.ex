defmodule Wadm.Observer.Cache do
  require Logger

  # return empty for now
  # TODO: load from a real cache
  def load_lattice(prefix) when is_binary(prefix) do
    Logger.debug("Fetching observed lattice state ('#{prefix}') from cache")
    key = "wadmcache:#{prefix}"

    case Redix.command(:redis_cache, ["GET", key]) do
      {:ok, value} ->
        if value == nil do
          nil
        else
          :erlang.binary_to_term(value)
        end

      {:error, e} ->
        Logger.error("Failed to retrieve lattice state from cache: #{inspect(e)}")
        nil
    end
  end

  # Writes an observed lattice to distributed cache
  def write_lattice(lattice = %LatticeObserver.Observed.Lattice{}) do
    key = "wadmcache:#{lattice.id}"
    value = :erlang.term_to_binary(lattice)

    case Redix.command(:redis_cache, ["SET", key, value]) do
      {:ok, _} ->
        Logger.debug("Cached lattice state: #{lattice.id}")

      {:error, e} ->
        Logger.error("Failed to load cached lattice: #{inspect(e)}")
    end

    nil
  end
end
