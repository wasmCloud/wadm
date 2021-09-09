defmodule LatticeControllerEngine.Reconciler.AppSpec do
  alias LatticeControllerEngine.Reconciler.Traits

  def reconcile(
        desired = %LatticeControllerEngine.Model.AppSpec{},
        actual = %LatticeControllerEngine.Observed.Lattice{}
      ) do
    for component <- desired.components,
        trait <- component.traits do
      Traits.reconcile_trait(desired, component, trait, actual)
    end
    |> List.flatten()
  end

  def matching_hosts(actual = %LatticeControllerEngine.Observed.Lattice{}, requirements = %{}) do
    actual.hosts
    |> Enum.filter(fn {_host_id, host} -> host.labels == requirements end)
    |> Enum.map(fn {host_id, _host} -> host_id end)
  end
end
