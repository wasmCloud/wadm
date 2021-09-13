defmodule LatticeControllerEngine.Model.SpreadScaler do
  alias __MODULE__
  alias LatticeControllerEngine.Model.WeightedTarget

  defstruct [:replicas, :spread]

  @typedoc """
    Type that represents a definition of a spread scaler in a trait bound to a component
  """
  @type t :: %SpreadScaler{
          replicas: integer(),
          spread: [WeightedTarget.t()]
        }
end
