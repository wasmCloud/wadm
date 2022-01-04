defmodule Wadm.Observer.NatsListener do
  require Logger
  use Gnat.Server

  def request(%{topic: topic, body: body}) do
    lattice_prefix =
      topic
      |> String.split(".")
      |> Enum.at(2)

    case Jason.decode(body) do
      {:ok, event} ->
        case Cloudevents.from_map(event) do
          {:ok, evt} ->
            # We have to iterate over horde supervisor children because
            # horde registries don't let us use duplicate keys, which means
            # we can't do multi-dispatch
            #
            # CAVEAT: the warning about `which_children` for large numbers of children
            # under low memory conditions applies doubly so for a Horde dynamic supervisor.
            Task.start(fn ->
              Horde.DynamicSupervisor.which_children(Wadm.HordeSupervisor)
              |> Enum.filter(fn {_, _pid, _type, [module]} ->
                module == Wadm.Deployments.DeploymentMonitor
              end)
              |> Enum.each(fn {_d, pid, _type, _modules} ->
                if Wadm.Deployments.DeploymentMonitor.lattice_prefix(pid) == lattice_prefix do
                  send(pid, {:handle_event, evt})
                end
              end)
            end)

          {:error, error} ->
            Logger.error("Failed to de-serialize cloud event: #{inspect(error)}")
        end

      {:error, error} ->
        Logger.error("Failed to decode event: #{inspect(error)}")
    end

    :ok
  end
end
