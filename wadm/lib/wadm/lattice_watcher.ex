defmodule Wadm.LatticeWatcher do
  require Logger
  @behaviour LatticeObserver.Observer

  def state_changed(old_state, new_state, event, lattice_prefix) do
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect old_state}")
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect new_state}")
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect event}")
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect lattice_prefix}")
    :ok
  end
end
