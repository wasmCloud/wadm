defmodule Wadm.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  def init(_opts) do
    config = Vapor.load!(Wadm.ConfigPlan)
    :ets.new(:config_table, [:named_table, :set, :public])
    :ets.insert(:config_table, {:config, config})

    topologies = [
      wadmcluster: [
        strategy: Cluster.Strategy.Gossip,
        config: [
          port: config.cluster.gossip_port |> parse_gossip_port()
        ]
      ]
    ]

    children = [
      {Cluster.Supervisor, [topologies, [name: Wadm.ClusterSupervisor]]},
      {Horde.DynamicSupervisor,
       [name: Wadm.HordeSupervisor, strategy: :one_for_one, members: :auto]},
      {Horde.Registry, [name: Wadm.HordeRegistry, keys: :unique, members: :auto]},
      {Phoenix.PubSub, name: Wadm.PubSub},
      Supervisor.child_spec(
        {Gnat.ConnectionSupervisor, Wadm.Api.Connection.settings_from_config(config)},
        id: :api_connection_supervisor
      ),
      Supervisor.child_spec(
        {Redix, host: config.redis.host, name: :model_store},
        id: :model_store
      ),
      Supervisor.child_spec(
        {Gnat.ConsumerSupervisor,
         %{
           connection_name: :api_nats,
           module: Wadm.Api.ApiServer,
           subscription_topics: [
             %{topic: "wadm.api.>", queue_group: "wadm_api_server"}
           ]
         }},
        id: :wadm_api_consumer_supervisor
      )
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def get_config() do
    case :ets.lookup(:config_table, :config) do
      [config: config_map] -> config_map
      _ -> nil
    end
  end

  # Helper function to parse a port number from a string or a number
  defp parse_gossip_port(num) when is_binary(num), do: num |> String.to_integer()
  defp parse_gossip_port(num) when is_integer(num), do: num
  # Fallback to default
  defp parse_gossip_port(_num), do: 45892
end
