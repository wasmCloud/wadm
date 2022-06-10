defmodule Wadm.Model.SpreadScaler do
  alias __MODULE__
  alias Wadm.Model.WeightedTarget

  @derive Jason.Encoder
  defstruct [:replicas, :spread]

  @typedoc """
    Type that represents a definition of a spread scaler in a trait bound to a component
  """
  @type t :: %SpreadScaler{
          replicas: integer(),
          spread: [WeightedTarget.t()]
        }
end
