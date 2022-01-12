defmodule Wadm.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  def init(_opts) do
    config = Vapor.load!(Wadm.ConfigPlan)

    topologies = [
      wadmcluster: [
        strategy: Cluster.Strategy.Gossip
      ]
    ]

    gnat_supervisor_settings = %{
      name: :gnats_connection_supervisor,
      # number of milliseconds to wait between consecutive reconnect attempts (default: 2_000)
      backoff_period: config.nats.backoff_period,
      connection_settings: [
        %{host: config.nats.host, port: config.nats.port}
      ]
    }

    children = [
      {Cluster.Supervisor, [topologies, [name: Wadm.ClusterSupervisor]]},
      # Mnesiac must be started after the clustering library starts https://github.com/beardedeagle/mnesiac#clustering
      {Mnesiac.Supervisor, [Node.list(), [name: Wadm.MnesiacSupervisor]]},
      {Horde.Registry, [name: Wadm.HordeRegistry, keys: :unique]},
      # The name of this horde supervisor corresponds to the horde parameter
      # in Horde.Cluster.members()
      {Horde.DynamicSupervisor,
       [name: Wadm.HordeSupervisor, strategy: :one_for_one, members: :auto]},
      {Redix, host: config.redis.host, name: :redis_cache},

      # TODO - right now we have one nats connection for all lattice observers. That
      # should be configurable to get NATS connections on a per-lattice basis
      Supervisor.child_spec(
        {Gnat.ConnectionSupervisor, gnat_supervisor_settings},
        id: :gnats_connection_supervisor
      )
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
