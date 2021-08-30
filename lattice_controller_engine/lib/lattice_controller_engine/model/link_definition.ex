defmodule LatticeControllerEngine.Model.LinkDefinition do
  alias __MODULE__
  @linkdefinition_type "linkdef"

  @typedoc """
    A type that represents a link definition specification, the details of which can be
    determined by looking up the target as a component of type "capability"
  """
  @type t :: %LinkDefinition{
          target: String.t(),
          values: Map.t(),
          type: String.t()
        }

  defstruct [:target, :values, type: @linkdefinition_type]
end
