import Config

config :logger, :console, format: "$time $metadata[$level] $message\n"

import_config "#{config_env()}.exs"
