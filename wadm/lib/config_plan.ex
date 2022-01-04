defmodule Wadm.ConfigPlan do
  use Vapor.Planner
  dotenv()

  config :nats,
         env([
           {:host, "NATS_HOST", default: '127.0.0.1'},
           {:port, "NATS_PORT", default: 4222},
           {:backoff_period, "NATS_BACKOFF_PERIOD", default: 4_000}
         ])

  config :redis,
         env([
           {:host, "REDIS_HOST", default: "localhost"}
         ])

  config :lattice,
         env([
           {:prefix, "LATTICE_PREFIX", default: "default"}
         ])
end
