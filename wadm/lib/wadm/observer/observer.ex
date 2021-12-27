defmodule Wadm.Observer do
  @doc """
  This module receives event publications from a lattice observer from the
  lattice_observer library. That library does no pre-filtering, so it's up to
  us to determine if the downstream consumers of this information need to know.

  It's important to note that this module isn't stateful nor is it a genserver.
  There will be 1 monitor process per lattice prefix, so the more of those that exist,
  the more lattice traffic will pass through this 'state_changed' function.
  """
  use LatticeObserver.Observer
  import Wadm.Deployments.DeploymentMonitor, only: [is_reconciliation_ready?: 1]
  require Logger

  @impl true
  def state_changed(old_state, new_state, event, lattice_prefix) do
    if new_state != old_state &&
         is_relevant?(event) &&
         should_reconcile?(event) do
      # Dispatch the new state to all deployment managers listening on this
      # lattice.
      Registry.dispatch(Registry.DeploymentsByLatticeRegistry, lattice_prefix, fn entries ->
        for {pid, _} <- entries do
          # TODO - this looks a LOT like what we'd do with GenStage
          # look into upgrading this so it triggers a GenStage flow?
          if is_reconciliation_ready?(pid) do
            send(pid, {:handle_event, new_state, event})
          end
        end
      end)
    end

    :ok
  end

  # TODO - make real
  def is_relevant?(_event), do: true
  # TODO - make real
  def should_reconcile?(_event), do: true
end
