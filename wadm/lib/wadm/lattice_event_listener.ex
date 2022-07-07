defmodule Wadm.LatticeEventListener do
  alias Phoenix.PubSub
  require Logger
  use Gnat.Server

  def request(%{topic: topic, body: body}) do
    lattice_id =
      topic
      |> String.split(".")
      |> List.last()

    Logger.debug("Event inbound for lattice #{lattice_id}")

    with {:ok, event} <- Jason.decode(body),
         {:ok, cloud_event} <- Cloudevents.from_map(event) do
      dispatch_cloud_event(lattice_id, cloud_event)
    else
      {:error, e} ->
        Logger.error("Failed to decode cloud event from message: #{inspect(e)}")
    end
  end

  defp dispatch_cloud_event(lattice_id, cloud_event) do
    PubSub.broadcast(Wadm.PubSub, "lattice:#{lattice_id}", {:cloud_event, cloud_event})
  end
end
