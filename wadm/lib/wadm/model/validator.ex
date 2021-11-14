defmodule Wadm.Model.Validator do
  alias Wadm.Model.{
    AppSpec,
    ActorComponent,
    CapabilityComponent,
    SpreadScaler,
    LinkDefinition
  }

  @doc """
  Validates an application specification structure. This validation only tests against conditions
  that are testable from within a single specification.
  """
  @spec validate_appspec(Wadm.Model.AppSpec.t()) ::
          :ok | {:error, [String.t()]}
  def validate_appspec(%AppSpec{} = appspec) do
    result = %{
      spec: appspec,
      errors: []
    }

    result
    |> validate_components()
  end

  defp validate_components(result) do
    result =
      result.spec.components
      |> Enum.reduce(result, fn comp, acc -> validate_component(comp, acc) end)

    if length(result.errors) > 0 do
      {:error, result.errors}
    else
      :ok
    end
  end

  defp validate_component(%ActorComponent{} = comp, result) do
    comp.traits
    |> Enum.reduce(result, fn trait, acc -> validate_trait(trait, acc) end)
  end

  defp validate_component(%CapabilityComponent{} = comp, result) do
    comp.traits
    |> Enum.reduce(result, fn trait, acc -> validate_trait(trait, acc) end)
  end

  defp validate_trait(%LinkDefinition{} = trait, result) do
    targets = find_capability_component(result.spec, trait.target)

    cond do
      length(targets) > 1 ->
        %{
          result
          | errors: [
              "Too many targets matching link definition target #{trait.target}" | result.errors
            ]
        }

      length(targets) == 0 ->
        %{
          result
          | errors: [
              "No matching targets found for link definition target #{trait.target}"
              | result.errors
            ]
        }

      true ->
        result
    end
  end

  defp validate_trait(%SpreadScaler{} = trait, result) do
    weight_total =
      trait.spread
      |> Enum.map(& &1.weight)
      |> Enum.sum()

    if weight_total != 100 do
      %{result | errors: ["Spread scaler weight does not add up to 100" | result.errors]}
    else
      result
    end
  end

  defp find_capability_component(%AppSpec{components: comps}, comp_name)
       when is_binary(comp_name) do
    comps
    |> Enum.filter(fn comp ->
      case comp do
        %CapabilityComponent{name: name} -> name == comp_name
        _ -> false
      end
    end)
  end
end
