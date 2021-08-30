defmodule LatticeControllerEngine.Model.Decoder do
  @capability_component_type "capability"
  @actor_component_type "actor"

  alias LatticeControllerEngine.Model.{
    ActorComponent,
    CapabilityComponent,
    LinkDefinition,
    SpreadScaler,
    WeightedTarget,
    AppSpec
  }

  @doc """
  Takes a map as returned by either of `YamlElixir`'s parse functions and returns
  either an error or a canonical representation of the components discovered within
  the model. Component extraction is an all-or-nothing process - if one of the components
  in the model map fails to decode, then the entire operation will fail
  """
  @spec extract_components(Map.t()) :: {:ok, [AppSpec.component()]} | {:error, String.t()}
  def extract_components(yaml) do
    case get_in(yaml, ["spec", "components"]) do
      nil ->
        {:error, "No components to extract from application specification"}

      comps ->
        {pass, fail} =
          comps
          |> Enum.map(fn comp -> from_map(comp) end)
          |> Enum.split_with(fn x ->
            case x do
              {:ok, _c} -> true
              {:error, _e} -> false
            end
          end)

        if length(fail) == 0 do
          {:ok,
           pass
           |> Enum.map(fn {:ok, x} -> x end)}
        else
          reasons = fail |> Enum.map(fn {:error, r} -> r end) |> Enum.join(",")
          {:error, "Failed to extract components: #{reasons}"}
        end
    end
  end

  defp from_map(
         %{
           "name" => name,
           "type" => @capability_component_type,
           "properties" => %{
             "contract" => contract,
             "image" => image
             # "link_name" => link_name
           }
           # "traits" => traits
         } = map
       ) do
    traits =
      case map["traits"] do
        nil -> []
        t -> t
      end

    link_name =
      case get_in(map, ["properties", "link_name"]) do
        nil -> "default"
        ln -> ln
      end

    case extract_traits(traits) do
      {:ok, traits} ->
        {:ok,
         %CapabilityComponent{
           name: name,
           image: image,
           contract: contract,
           link_name: link_name,
           traits: traits
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp from_map(%{
         "name" => name,
         "type" => @actor_component_type,
         "properties" => %{
           "image" => image
         },
         "traits" => traits
       }) do
    case extract_traits(traits) do
      {:ok, traits} ->
        {:ok,
         %ActorComponent{
           name: name,
           image: image,
           traits: traits
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp from_map(%{
         "name" => name,
         "type" => @actor_component_type,
         "properties" => %{
           "image" => image
         }
       }) do
    {:ok,
     %ActorComponent{
       name: name,
       image: image,
       traits: []
     }}
  end

  defp from_map(%{"type" => @actor_component_type}) do
    {:error, "Cannot extract actor component from map"}
  end

  defp from_map(%{"type" => @capability_component_type}) do
    {:error, "Cannot extract capability component from map"}
  end

  defp extract_traits(traits) do
    {pass, fail} =
      traits
      |> Enum.map(fn trait -> trait_from_map(trait) end)
      |> Enum.split_with(fn x ->
        case x do
          {:ok, _t} -> true
          {:error, _e} -> false
        end
      end)

    if length(fail) == 0 do
      {:ok,
       pass
       |> Enum.map(fn {:ok, x} -> x end)}
    else
      {:error, "Failed to extract traits"}
    end
  end

  defp trait_from_map(%{
         "type" => "spreadscaler",
         "properties" => %{
           "replicas" => replicas,
           "spread" => weighted_targets
         }
       }) do
    {pass, fail} =
      weighted_targets
      |> Enum.map(fn target -> target_from_map(target) end)
      |> Enum.split_with(fn x ->
        case x do
          {:ok, _t} -> true
          {:error, _e} -> false
        end
      end)

    if length(fail) == 0 do
      {:ok,
       %SpreadScaler{
         replicas: replicas,
         spread:
           pass
           |> Enum.map(fn {:ok, x} -> x end)
       }}
    else
      {:error, "Failed to extract weighted targets from spread definition"}
    end
  end

  defp trait_from_map(%{
         "type" => "linkdef",
         "properties" => %{
           "target" => target,
           "values" => values
         }
       }) do
    {:ok,
     %LinkDefinition{
       target: target,
       values: values
     }}
  end

  defp trait_from_map(%{}) do
    {:error, "Unable to decode trait from map"}
  end

  defp target_from_map(
         %{
           "name" => name,
           "requirements" => requirements
         } = map
       )
       when is_map(requirements) do
    weight =
      case map["weight"] do
        nil -> 100
        w -> w
      end

    {:ok,
     %WeightedTarget{
       name: name,
       requirements: requirements,
       weight: weight
     }}
  end

  defp target_from_map(%{}) do
    {:error, "Unable to decode weighted target from spread specification"}
  end
end
