import Config

config :logger,
  compile_time_purge_matching: [
    [application: :gnat],
    [level_lower_than: :info]
  ]

config :logger, :console, format: "$time $metadata[$level] $message\n", level: :debug

import_config "#{config_env()}.exs"
