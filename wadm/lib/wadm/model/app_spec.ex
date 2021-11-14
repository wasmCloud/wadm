defmodule Wadm.Model.AppSpec do
  @moduledoc """
  The root of an OAM Application Specification model
  """
  alias __MODULE__

  alias Wadm.Model.{
    ActorComponent,
    CapabilityComponent,
    Decoder
  }

  @enforce_keys [:name]
  defstruct [:name, :version, :description, components: []]

  @typedoc """
    Valid component types
  """
  @type component :: ActorComponent.t() | CapabilityComponent.t()

  @typedoc """
    The root model specification for an application to be managed by the controller
  """
  @type t :: %AppSpec{
          name: String.t(),
          version: String.t(),
          description: String.t(),
          components: [component()]
        }

  @doc """
  Creates a new wasmCloud OAM application specification
  """
  @spec new(String.t(), String.t(), String.t(), [component()]) :: AppSpec.t()
  def new(name, version, description, components \\ []) do
    %AppSpec{
      name: name,
      version: version,
      description: description,
      components: components
    }
  end

  @doc """
  Takes a map as returned by either of `YamlElixir`'s parse functions and returns either
  a canonical representation of the wasmCloud OAM application specification model or
  an error and an accompanying reason indicating the cause of the decode failure
  """
  @spec from_yaml(Map.t()) :: {:ok, AppSpec.t()} | {:error, String.t()}
  def from_yaml(yaml = %{}) do
    case Decoder.extract_components(yaml) do
      {:ok, components} ->
        {:ok,
         new(
           case get_in(yaml, ["metadata", "name"]) do
             nil -> "Unnamed"
             n -> n
           end,
           case get_in(yaml, ["metadata", "annotations", "version"]) do
             nil -> "v0.0.0"
             v -> v
           end,
           case get_in(yaml, ["metadata", "annotations", "description"]) do
             nil -> "Unnamed Application"
             d -> d
           end,
           components
         )}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
