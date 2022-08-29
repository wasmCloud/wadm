defmodule Wadm.Reconciler.AppSpec do
  alias Wadm.Reconciler.Traits

  def reconcile(
        desired = %Wadm.Model.AppSpec{},
        actual = %LatticeObserver.Observed.Lattice{}
      ) do
    for component <- desired.components,
        trait <- component.traits do
      Traits.reconcile_trait(desired, component, trait, actual)
    end
    |> List.flatten()
  end

  # Matches all hosts when no requirements are specified (or empty requirements)
  def matching_hosts(actual = %LatticeObserver.Observed.Lattice{}, requirements)
      when requirements == %{} or requirements == nil do
    actual.hosts
    |> Enum.map(fn {host_id, _host} -> host_id end)
  end

  def matching_hosts(actual = %LatticeObserver.Observed.Lattice{}, requirements) do
    actual.hosts
    |> Enum.filter(fn {_host_id, host} -> subset?(requirements, host.labels) end)
    |> Enum.map(fn {host_id, _host} -> host_id end)
  end

  defp subset?(map1, map2) do
    in_other = fn {key, value} ->
      {:ok, value} == Map.fetch(map2, key)
    end

    map1
    |> Map.to_list()
    |> Enum.all?(in_other)
  end
end
