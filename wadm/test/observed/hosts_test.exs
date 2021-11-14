defmodule WadmTest.Observed.HostsTest do
  use ExUnit.Case
  alias Wadm.Observed.Lattice
  alias TestSupport.CloudEvents

  @test_host "Nxxx"

  describe "Observed Lattice Monitors Host Events" do
    test "Properly records host heartbeat" do
      hb = CloudEvents.host_heartbeat(@test_host, %{foo: "bar", baz: "biz"})
      stamp = Lattice.timestamp_from_iso8601(hb.time)
      l = Lattice.apply_event(Lattice.new(), hb)

      assert l ==
               %Wadm.Observed.Lattice{
                 actors: %{},
                 hosts: %{
                   "Nxxx" => %Wadm.Observed.Host{
                     id: "Nxxx",
                     labels: %{baz: "biz", foo: "bar"},
                     last_seen: stamp
                   }
                 },
                 ocimap: %{},
                 instance_tracking: %{},
                 linkdefs: [],
                 providers: %{}
               }

      hb2 = CloudEvents.host_heartbeat(@test_host, %{foo: "bar", baz: "biz"})
      stamp2 = Lattice.timestamp_from_iso8601(hb2.time)
      l = Lattice.apply_event(l, hb2)

      assert l ==
               %Wadm.Observed.Lattice{
                 actors: %{},
                 hosts: %{
                   "Nxxx" => %Wadm.Observed.Host{
                     id: "Nxxx",
                     labels: %{baz: "biz", foo: "bar"},
                     last_seen: stamp2
                   }
                 },
                 ocimap: %{},
                 instance_tracking: %{},
                 linkdefs: [],
                 providers: %{}
               }
    end
  end
end
