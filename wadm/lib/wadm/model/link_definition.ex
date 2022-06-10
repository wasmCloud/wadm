defmodule Wadm.Model.LinkDefinition do
  alias __MODULE__
  alias Wadm.Model.{AppSpec, CapabilityComponent}

  @typedoc """
    A type that represents a link definition specification, the details of which can be
    determined by looking up the target as a component of type "capability"
  """
  @type t :: %LinkDefinition{
          target: String.t(),
          values: Map.t()
        }

  @derive Jason.Encoder
  defstruct [:target, :values]

  @spec resolve_target(
          Wadm.Model.LinkDefinition.t(),
          Wadm.Model.AppSpec.t()
        ) :: {:ok, CapabilityComponent.t()} | :error
  def resolve_target(%LinkDefinition{target: target}, %AppSpec{components: comps})
      when is_binary(target) do
    case comps
         |> Enum.filter(fn component -> component.name == target end)
         |> List.first() do
      nil -> :error
      ld -> {:ok, ld}
    end
  end
end
