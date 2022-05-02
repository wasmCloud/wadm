defmodule Wadm.ConfigPlan do
  use Vapor.Planner
  dotenv()

  config :nats,
         env([
           {:api_host, "API_NATS_HOST", default: '127.0.0.1'},
           {:api_port, "API_NATS_PORT", default: 4222},
           {:api_tls, "API_TLS", default: "false"},
           {:api_jwt, "API_USER_JWT", default: ""},
           {:api_seed, "API_USER_SEED", default: ""},
           {:api_backoff_period, "NATS_BACKOFF_PERIOD", default: 4_000}
         ])

  config :redis,
         env([
           {:host, "REDIS_HOST", default: "localhost"}
         ])
end
