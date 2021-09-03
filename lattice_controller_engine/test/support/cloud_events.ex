defmodule TestSupport.CloudEvents do
  @appspec "wasmcloud.dev/appspec"

  def actor_started(pk, instance_id, spec, host) do
    %{"public_key" => pk, "instance_id" => instance_id, "annotations" => %{@appspec => spec}}
    |> LatticeControllerEngine.CloudEvent.new("actor_started", host)
  end

  def actor_stopped(pk, instance_id, spec, host) do
    %{"public_key" => pk, "instance_id" => instance_id, "annotations" => %{@appspec => spec}}
    |> LatticeControllerEngine.CloudEvent.new("actor_stopped", host)
  end

  def provider_started(pk, contract_id, link_name, instance_id, spec, host) do
    %{
      "public_key" => pk,
      "instance_id" => instance_id,
      "link_name" => link_name,
      "contract_id" => contract_id,
      "annotations" => %{@appspec => spec}
    }
    |> LatticeControllerEngine.CloudEvent.new("provider_started", host)
  end

  def provider_stopped(pk, contract_id, link_name, instance_id, spec, host) do
    %{
      "public_key" => pk,
      "instance_id" => instance_id,
      "link_name" => link_name,
      "contract_id" => contract_id,
      "annotations" => %{@appspec => spec}
    }
    |> LatticeControllerEngine.CloudEvent.new("provider_stopped", host)
  end

  def host_heartbeat(host, labels) do
    %{
      "actors" => [],
      "providers" => [],
      "labels" => labels
    }
    |> LatticeControllerEngine.CloudEvent.new("host_heartbeat", host)
  end
end
