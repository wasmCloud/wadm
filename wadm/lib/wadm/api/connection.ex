defmodule Wadm.Api.Connection do
  require Logger

  def settings_from_config(config) do
    %{
      name: :api_nats,
      # number of milliseconds to wait between consecutive reconnect attempts (default: 2_000)
      backoff_period: config.nats.api_backoff_period,
      connection_settings: [
        Map.merge(
          %{
            host: config.nats.api_host,
            port: config.nats.api_port,
            tls: config.nats.api_tls == "true"
          },
          determine_auth_method(config.nats.api_seed, config.nats.api_jwt)
        )
      ]
    }
  end

  defp determine_auth_method(nkey_seed, jwt) do
    cond do
      jwt != "" && nkey_seed != "" ->
        Logger.info("wadm API Server: Authenticating to NATS with JWT and seed")
        %{jwt: jwt, nkey_seed: nkey_seed, auth_required: true}

      nkey_seed != "" ->
        Logger.info("wadm API Server: Authenticating to NATS with seed only")
        %{nkey_seed: nkey_seed, auth_required: true}

      # No arguments specified that create a valid authentication method
      true ->
        Logger.info("wadm API Server: Authenticating to NATS anonymously")
        %{}
    end
  end
end
