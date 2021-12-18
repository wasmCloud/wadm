defmodule Wadm.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  def init(_opts) do
    config = Vapor.load!(Wadm.ConfigPlan)

    gnat_supervisor_settings = %{
      name: :gnat,
      # number of milliseconds to wait between consecutive reconnect attempts (default: 2_000)
      backoff_period: config.nats.backoff_period,
      connection_settings: [
        %{host: config.nats.host, port: config.nats.port}
      ]
    }

    children = [
      Supervisor.child_spec(
        {Gnat.ConnectionSupervisor, gnat_supervisor_settings},
        id: :gnats_connection_supervisor
      )
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end
