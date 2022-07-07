defmodule WadmTest.ConfigPlan do
  use ExUnit.Case

  test "vaportest" do
    config = Vapor.load!(Wadm.ConfigPlan)
    assert config.nats.api_host == '127.0.0.1'
    assert config.nats.api_port == 4222
  end
end
