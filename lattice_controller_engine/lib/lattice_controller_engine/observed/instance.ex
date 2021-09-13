defmodule LatticeControllerEngine.Observed.Instance do
  alias __MODULE__

  @enforce_keys [:id, :host_id, :spec_id]
  defstruct [:id, :host_id, :spec_id]

  @typedoc """
  An instance represents an observation of a unit of scalability within the lattice. Instances
  have unique IDs (GUIDs) and are stored along with the host on which they exist and the ID of
  the specification (`AppSpec` model) responsible for that instance. Actors and Capability Providers
  both have instances while link definitions do not.
  """
  @type t :: %Instance{
          id: String.t(),
          host_id: String.t(),
          spec_id: String.t()
        }
end
