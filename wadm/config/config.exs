use Mix.Config

config :logger, :console, format: "$time $metadata[$level] $message\n"

import_config "#{Mix.env()}.exs"
