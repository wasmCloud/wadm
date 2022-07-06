defmodule Wadm.Reconciler.Traits do
  alias Wadm.Model.{
    AppSpec,
    ActorComponent,
    SpreadScaler,
    CapabilityComponent,
    LinkDefinition
  }

  alias Wadm.Reconciler.SpreadScaler, as: SpreadScalerReconciler
  alias Wadm.Reconciler.Command
  alias LatticeObserver.Observed

  def reconcile_trait(
        spec = %AppSpec{},
        component = %ActorComponent{},
        scaler = %SpreadScaler{},
        actual = %Observed.Lattice{}
      ) do
    pk =
      case Observed.Lattice.lookup_ociref(actual, component.image) do
        {:ok, actor_id} -> actor_id
        _ -> nil
      end

    SpreadScalerReconciler.perform_spread(spec.name, component.image, pk, scaler, actual)
  end

  def reconcile_trait(
        spec = %AppSpec{},
        component = %ActorComponent{},
        ld = %LinkDefinition{},
        actual = %Observed.Lattice{}
      ) do
    with {:ok, cap} <- LinkDefinition.resolve_target(ld, spec),
         {:ok, actor_id} <- Observed.Lattice.lookup_ociref(actual, component.image),
         {:ok, provider_id} <- Observed.Lattice.lookup_ociref(actual, cap.image) do
      case Observed.Lattice.lookup_linkdef(actual, actor_id, provider_id, cap.link_name) do
        {:ok, _} ->
          [Command.new(:no_action, "Link definition already exists", %{})]

        :error ->
          [
            Command.new(:put_linkdef, "", %{
              ld: %Observed.LinkDefinition{
                actor_id: actor_id,
                provider_id: provider_id,
                contract_id: cap.contract,
                link_name: cap.link_name,
                values: ld.values
              }
            })
          ]
      end
    else
      _ ->
        [
          Command.new(
            :no_action,
            "Link definition target '#{ld.target}' or source '#{component.name}' may not be running in the lattice",
            %{}
          )
        ]
    end
  end

  def reconcile_trait(
        spec = %AppSpec{},
        component = %CapabilityComponent{},
        scaler = %SpreadScaler{},
        actual = %Observed.Lattice{}
      ) do
    pk =
      case Observed.Lattice.lookup_ociref(actual, component.image) do
        {:ok, provider_id} -> provider_id
        _ -> nil
      end

    SpreadScalerReconciler.perform_spread(spec.name, component.image, pk, scaler, actual, 1, %{
      link_name: component.link_name,
      contract_id: component.contract
    })
  end
end
