defmodule Wadm.Nats do
  @moduledoc false
  require Logger

  # Heavily inspired from the flawless https://github.com/wasmCloud/wasmcloud-otp/blob/main/host_core/lib/host_core/nats.ex
  def safe_pub(process_name, topic, msg) do
    if Process.whereis(process_name) != nil do
      Gnat.pub(process_name, topic, msg)
    else
      Logger.warning("Publication on #{topic} aborted - connection #{process_name} is down",
        nats_topic: topic
      )
    end
  end

  def safe_req(process_name, topic, body, opts \\ []) do
    if Process.whereis(process_name) != nil do
      Gnat.request(process_name, topic, body, opts)
    else
      Logger.error(
        "NATS request for #{topic} aborted, connection #{process_name} is down. Returning 'fast timeout'",
        nats_topic: topic
      )

      {:error, :timeout}
    end
  end
end
