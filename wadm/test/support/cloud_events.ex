defmodule TestSupport.CloudEvents do
  @appspec "wasmcloud.dev/appspec"

  def actor_started(pk, instance_id, spec, host) do
    %{"public_key" => pk, "instance_id" => instance_id, "annotations" => %{@appspec => spec}}
    |> Wadm.CloudEvent.new("actor_started", host)
  end

  def actor_stopped(pk, instance_id, spec, host) do
    %{"public_key" => pk, "instance_id" => instance_id, "annotations" => %{@appspec => spec}}
    |> Wadm.CloudEvent.new("actor_stopped", host)
  end

  def provider_started(pk, contract_id, link_name, instance_id, spec, host) do
    %{
      "public_key" => pk,
      "instance_id" => instance_id,
      "link_name" => link_name,
      "contract_id" => contract_id,
      "annotations" => %{@appspec => spec}
    }
    |> Wadm.CloudEvent.new("provider_started", host)
  end

  def provider_stopped(pk, contract_id, link_name, instance_id, spec, host) do
    %{
      "public_key" => pk,
      "instance_id" => instance_id,
      "link_name" => link_name,
      "contract_id" => contract_id,
      "annotations" => %{@appspec => spec}
    }
    |> Wadm.CloudEvent.new("provider_stopped", host)
  end

  def host_heartbeat(host, labels) do
    %{
      "actors" => [],
      "providers" => [],
      "labels" => labels
    }
    |> Wadm.CloudEvent.new("host_heartbeat", host)
  end

  def linkdef_put(actor_id, provider_id, link_name, contract_id, values, host) do
    %{
      "actor_id" => actor_id,
      "provider_id" => provider_id,
      "link_name" => link_name,
      "contract_id" => contract_id,
      "values" => values
    }
    |> Wadm.CloudEvent.new("linkdef_put", host)
  end

  def linkdef_del(actor_id, provider_id, link_name, host, contract_id) do
    %{
      "actor_id" => actor_id,
      "provider_id" => provider_id,
      "link_name" => link_name,
      "contract_id" => contract_id
    }
    |> Wadm.CloudEvent.new("linkdef_del", host)
  end
end
