defmodule WadmTest.Observed.HostsTest do
  use ExUnit.Case
  alias LatticeObserver.Observed.{Lattice, Host}
  alias TestSupport.CloudEvents

  @test_host "Nxxx"

  describe "Observed Lattice Monitors Host Events" do
    test "Properly records host heartbeat" do
      hb = CloudEvents.host_heartbeat(@test_host, %{foo: "bar", baz: "biz"})
      stamp = Lattice.timestamp_from_iso8601(hb.time)
      l = Lattice.apply_event(Lattice.new(), hb)

      assert l ==
               %Lattice{
                 Lattice.new()
                 | hosts: %{
                     "Nxxx" => %Host{
                       id: "Nxxx",
                       labels: %{baz: "biz", foo: "bar"},
                       last_seen: stamp,
                       first_seen: stamp,
                       status: :healthy
                     }
                   }
               }

      hb2 = CloudEvents.host_heartbeat(@test_host, %{foo: "bar", baz: "biz"})
      stamp2 = Lattice.timestamp_from_iso8601(hb2.time)
      l = Lattice.apply_event(l, hb2)

      assert l ==
               %Lattice{
                 Lattice.new()
                 | hosts: %{
                     "Nxxx" => %Host{
                       id: "Nxxx",
                       labels: %{baz: "biz", foo: "bar"},
                       status: :healthy,
                       last_seen: stamp2,
                       first_seen: stamp
                     }
                   }
               }
    end
  end
end
