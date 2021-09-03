defmodule LatticeControllerEngine.Observed.Host do
  alias __MODULE__

  @enforce_keys [:id, :labels]
  defstruct [:id, :labels, :last_seen]

  @typedoc """
  Represents a host observed through heartbeat events within a lattice
  """
  @type t :: %Host{
          id: String.t(),
          labels: Map.t(),
          last_seen: Datetime.t()
        }
end
