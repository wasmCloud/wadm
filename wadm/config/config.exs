use Mix.Config

config :logger, :console, format: "$time $metadata[$level] $message\n"

config :mnesiac,
  stores: [Wadm.LatticeStore],
  # defaults to :ram_copies
  schema_type: :disc_copies,
  # milliseconds, default is 600_000
  table_load_timeout: 600_000

import_config "#{Mix.env()}.exs"
