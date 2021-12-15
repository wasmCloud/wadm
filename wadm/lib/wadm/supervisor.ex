defmodule Wadm.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, :ok, opts)
  end

  @impl true
  def init(:ok) do
    gnat_supervisor_settings = %{
      # (required) the registered named you want to give the Gnat connection
      name: :gnat,
      # number of milliseconds to wait between consecutive reconnect attempts (default: 2_000)
      backoff_period: 4_000,
      connection_settings: [
        %{host: '172.18.0.1', port: 4222}
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
