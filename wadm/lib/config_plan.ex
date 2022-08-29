defmodule Wadm.ConfigPlan do
  use Vapor.Planner
  dotenv()

  config :nats,
         env([
           {:api_host, "WADM_NATS_HOST", default: '127.0.0.1'},
           {:api_port, "WADM_NATS_PORT", default: 4222, map: &String.to_integer/1},
           {:api_tls, "WADM_TLS", default: "false"},
           {:api_jwt, "WADM_USER_JWT", default: ""},
           {:api_seed, "WADM_USER_SEED", default: ""},
           {:api_backoff_period, "WADM_NATS_BACKOFF_PERIOD", default: 4_000},
           {:ctl_topic_prefix, "WADM_CTL_TOPIC_PREFIX", default: "wasmbus.ctl"}
         ])

  config :cluster,
         env([
           {:gossip_port, "WADM_CLUSTER_GOSSIP_PORT", default: 45892}
         ])

  config :redis,
         env([
           {:host, "WADM_REDIS_HOST", default: "localhost"}
         ])

  config :secrets,
         env([
           {:vault_token, "WADM_VAULT_TOKEN", default: ""},
           {:vault_addr, "WADM_VAULT_ADDR", default: "http://127.0.0.1:8200"},
           {:vault_path_template, "WADM_VAULT_PATH_TEMPLATE", default: ""}
         ])
end
