defmodule Wadm.Reconciler.SpreadScaler do
  alias Wadm.Reconciler.Command
  alias Wadm.Model.{SpreadScaler, WeightedTarget}
  alias LatticeObserver.Observed.Lattice
  alias Wadm.Reconciler.AppSpec, as: AppSpecReconciler

  @type target_instance_map :: %{required(String.t()) => [Instance.t()]}

  @spec perform_spread(
          String.t(),
          String.t(),
          String.t() | nil,
          SpreadScaler.t(),
          Lattice.t(),
          integer()
        ) :: [Command.t()]
  def perform_spread(
        spec_name,
        image,
        component_pk,
        scaler = %SpreadScaler{},
        actual = %Lattice{},
        max_per_host \\ 1000,
        opts \\ %{}
      ) do
    for wtarget <- scaler.spread do
      candidate_hosts = AppSpecReconciler.matching_hosts(actual, wtarget.requirements)

      running_instances =
        Lattice.running_instances(actual, component_pk, spec_name)
        |> Enum.filter(fn %{host_id: hid} -> hid in candidate_hosts end)

      desired_instances = trunc(wtarget.weight / 100 * scaler.replicas)

      cond do
        length(running_instances) > desired_instances ->
          reduce_instances(image, component_pk, wtarget, running_instances, opts)

        length(running_instances) < desired_instances ->
          increase_instances(
            image,
            wtarget,
            running_instances,
            candidate_hosts,
            max_per_host,
            opts
          )

        true ->
          Command.new(:no_action, "Weighted target '#{wtarget.name}' is satisfied", opts)
      end
    end
  end

  @spec reduce_instances(String.t(), String.t(), WeightedTarget.t(), list(), Map.t()) ::
          Command.t()
  defp reduce_instances(image, pk, wtarget = %WeightedTarget{}, running_instances, opts) do
    # Pick the most populated host from among those hosts with running instances
    # of the entity managed by this spec
    candidate =
      running_instances
      |> Enum.group_by(fn instance -> instance.host_id end, fn instance -> instance end)
      |> Enum.sort(fn a, b -> length(elem(a, 1)) > length(elem(b, 1)) end)
      |> List.first()

    {tgt_host, _} = candidate

    Command.new(
      :stop,
      "Weighted target '#{wtarget.name}' has too many instances.",
      Map.merge(opts, %{
        image: image,
        host_id: tgt_host,
        id: pk
      })
    )
  end

  @spec increase_instances(
          String.t(),
          WeightedTarget.t(),
          list(),
          [String.t()],
          integer(),
          Map.t()
        ) ::
          Command.t()
  defp increase_instances(
         image,
         wtarget = %WeightedTarget{},
         running_instances,
         candidate_hosts,
         max_per_host,
         opts
       ) do
    # Take all of the candidate hosts and attach all running instances to those
    # hosts, then filter out all hosts that exceed the max_per_host value,
    # sort ascending by level of occupation (least occupied first)
    candidates =
      candidate_hosts
      |> Enum.map(fn host ->
        {host, running_instances |> Enum.filter(fn instance -> instance.host_id == host end)}
      end)
      |> Enum.filter(fn {_h, is} -> length(is) < max_per_host end)
      |> Enum.sort(fn a, b -> length(elem(a, 1)) < length(elem(b, 1)) end)

    if length(candidates) > 0 do
      {tgt_host, _} = candidates |> List.first()

      Command.new(
        :start,
        "Weighted target '#{wtarget.name}' needs more instances.",
        Map.merge(opts, %{
          host_id: tgt_host,
          image: image
        })
      )
    else
      Command.new(
        :error,
        "Weighted target '#{wtarget.name}' has insufficient candidate hosts.",
        opts
      )
    end
  end
end
