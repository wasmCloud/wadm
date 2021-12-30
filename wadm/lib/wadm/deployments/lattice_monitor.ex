defmodule Wadm.Deployments.LatticeMonitor do
  use GenServer
  require Logger

  import Wadm.Deployments.DeploymentMonitor, only: [via_tuple: 1]

  @spec start_link(Map.t()) :: GenServer.on_start()
  def start_link(opts) do
    case GenServer.start_link(__MODULE__, opts, name: via_tuple(opts.lattice_prefix)) do
      {:error, {:already_started, pid}} ->
        Logger.debug("Already running lattice monitor at #{inspect(pid)}")
        :ignore

      other ->
        other
    end
  end

  @impl true
  def init(opts) do
    Logger.debug("Starting lattice monitor for '#{opts.lattice_prefix}'")
    # Config NATS subscriber
    cs_settings = %{
      connection_name: :gnats_connection_supervisor,
      module: Wadm.Observer.NatsListener,
      subscription_topics: [
        %{
          topic: "wasmbus.evt.#{opts.lattice_prefix}"
        }
      ]
    }

    Logger.debug("Starting NATS subscriber for lattice '#{opts.lattice_prefix}'")
    {:ok, _super} = Gnat.ConsumerSupervisor.start_link(cs_settings, [])
  end
end
