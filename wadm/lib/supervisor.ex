defmodule Wadm.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  def init(_opts) do
    config = Vapor.load!(Wadm.ConfigPlan)

    gnat_supervisor_settings = %{
      name: :gnats_connection_supervisor,
      # number of milliseconds to wait between consecutive reconnect attempts (default: 2_000)
      backoff_period: config.nats.backoff_period,
      connection_settings: [
        %{host: config.nats.host, port: config.nats.port}
      ]
    }

    children = [
      {Registry, keys: :unique, name: Registry.DeploymentMonitorRegistry},
      {Registry, keys: :duplicate, name: Registry.DeploymentsByLatticeRegistry},
      # The deployment supervisor manages children of type DeploymentMonitor, where
      # each one is responsible for monitoring/managing a specific version of a deployed
      # appspec. Appspecs are "deployed" via PUT-like operation on the as-yet-unwritten
      # wadm API (via NATS)
      {Wadm.Deployments.Supervisor, []},
      # TODO - right now we have one nats connection for all lattice observers. That
      # should be configurable to get NATS connections on a per-lattice basis
      Supervisor.child_spec(
        {Gnat.ConnectionSupervisor, gnat_supervisor_settings},
        id: :gnats_connection_supervisor
      ),
      # There is a root lattice observer, to which LatticeObserver.NatsObserver
      # processes can be added as children, one to monitor each lattice prefix
      {Wadm.Observer.RootLatticeObserver, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
