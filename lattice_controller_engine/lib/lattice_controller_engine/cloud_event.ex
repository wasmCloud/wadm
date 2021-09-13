defmodule LatticeControllerEngine.CloudEvent do
  def new(data, event_type, host) do
    stamp = DateTime.utc_now() |> DateTime.to_iso8601()

    %{
      specversion: "1.0",
      time: stamp,
      type: "com.wasmcloud.lattice.#{event_type}",
      source: "#{host}",
      datacontenttype: "application/json",
      id: UUID.uuid4(),
      data: data
    }
    |> Cloudevents.from_map!()
  end
end
