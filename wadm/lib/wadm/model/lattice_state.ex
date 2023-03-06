defmodule Wadm.Model.LatticeState do
  alias __MODULE__
  alias Wadm.Model.{ActorComponent, CapabilityComponent, Host, LinkDefinition}

  @enforce_keys [:id, :actors, :providers, :hosts, :linkdefs, :claims]
  defstruct [
    :id,
    :actors,
    :providers,
    :hosts,
    :linkdefs
    :claims,
  ]

  @typedoc """
  Map of wasmCloud actors, keyed by ID
  """
  @type actormap :: %{required(binary()) => [ActorComponent.t()]}

  @typedoc """
  Map of wasmCloud CapabilityComponents (AKA providers), keyed by ID
  """
  @type providermap :: %{required(binary()) => [CapabilityComponent.t()]}

  @typedoc """
  Map of wasmCloud hosts, keyed by ID
  """
  @type hostmap :: %{required(binary()) => [CapabilityComponent.t()]}

  @typedoc """
  Map of LinkDefinitions, keyed by target (component name)
  """
  @type linkdefmap :: %{required(binary()) => %{[LinkDefinition.t()]}

  @typedoc """
  State that his held about a Lattice
  """
  @type t :: %LatticeState {
    id: binary(),
    actors: actormap(),
    providers: providermap(),
    hosts: hostmap(),
    linkdefs: linkdefmap(),
    }

end
