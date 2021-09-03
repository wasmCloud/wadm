defmodule LatticeControllerEngine.Observed.LinkDefinition do
  alias __MODULE__

  @enforce_keys [:actor_id, :provider_id, :contract_id, :link_name]
  defstruct [:actor_id, :provider_id, :contract_id, :link_name, :values]

  @typedoc """
  A representation of an observed link definition. Link definitions are considered
  a global entity and are always shareable by all `AppSpec` models. The link definition
  trait can be satisfied for an `AppSpec` model by the existence of the link definitition,
  without regard for _how_ the definition came into being.
  Link definitions are uniquely identified by the actor ID, provider ID, and link name.
  """
  @type t :: %LinkDefinition{
          actor_id: String.t(),
          provider_id: String.t(),
          contract_id: String.t(),
          link_name: String.t(),
          values: Map.t()
        }
end
