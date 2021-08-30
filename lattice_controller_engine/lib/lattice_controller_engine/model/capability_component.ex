defmodule LatticeControllerEngine.Model.CapabilityComponent do
  alias __MODULE__

  defstruct [:name, :contract, :image, link_name: "default", traits: []]

  @type trait :: SpreadScaler.t()

  @type t :: %CapabilityComponent{
          name: String.t(),
          image: String.t(),
          contract: String.t(),
          link_name: String.t(),
          traits: [trait()]
        }

  @spec new(String.t(), String.t(), String.t(), [trait()], String.t()) :: CapabilityComponent.t()
  def new(name, image, contract, traits \\ [], link_name \\ "default") do
    %CapabilityComponent{
      name: name,
      image: image,
      contract: contract,
      link_name: link_name,
      traits: traits
    }
  end
end
