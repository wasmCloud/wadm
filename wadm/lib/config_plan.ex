defmodule Wadm.ConfigPlan do
  use Vapor.Planner
  dotenv()

  config :nats,
         env([
           {:api_host, "WADM_NATS_HOST", default: '127.0.0.1'},
           {:api_port, "WADM_NATS_PORT", default: 4222},
           {:api_tls, "WADM_TLS", default: "false"},
           {:api_jwt, "WADM_USER_JWT", default: ""},
           {:api_seed, "WADM_USER_SEED", default: ""},
           {:api_backoff_period, "WADM_NATS_BACKOFF_PERIOD", default: 4_000}
         ])

  config :cluster,
         env([
           {:gossip_port, "WADM_CLUSTER_GOSSIP_PORT", default: 45892}
         ])

  config :redis,
         env([
           {:host, "WADM_REDIS_HOST", default: "localhost"}
         ])
end
