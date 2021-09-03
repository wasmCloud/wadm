defmodule LatticeControllerEngineTest.Observed.ProvidersTest do
  use ExUnit.Case
  alias LatticeControllerEngine.Observed.{Lattice, Instance, Provider}
  alias TestSupport.CloudEvents

  @test_spec "testapp"
  @test_spec_2 "othertestapp"
  @test_host "Nxxx"
  @test_contract "wasmcloud:test"

  describe "Observed Lattice Monitors Provider Events" do
    test "Adds and Removes Providers" do
      start =
        CloudEvents.provider_started(
          "Vxxx",
          @test_contract,
          "default",
          "abc123",
          @test_spec,
          @test_host
        )

      l = Lattice.new()
      l = Lattice.apply_event(l, start)
      stamp1 = Lattice.timestamp_from_iso8601(start.time)
      # ensure idempotence
      l = Lattice.apply_event(l, start)

      orig_desired = %Lattice{
        actors: %{},
        hosts: %{},
        instance_tracking: %{
          "abc123" => stamp1
        },
        linkdefs: [],
        providers: %{
          {"Vxxx", "default"} => %Provider{
            contract_id: "wasmcloud:test",
            id: "Vxxx",
            instances: [
              %Instance{
                host_id: "Nxxx",
                id: "abc123",
                spec_id: "testapp"
              }
            ],
            link_name: "default"
          }
        }
      }

      assert l == orig_desired

      stop =
        CloudEvents.provider_stopped(
          "Vxxx",
          @test_contract,
          "default",
          "abc123",
          @test_spec,
          @test_host
        )

      l = Lattice.apply_event(l, stop)

      desired = %Lattice{
        actors: %{},
        hosts: %{},
        instance_tracking: %{},
        linkdefs: [],
        providers: %{
          {"Vxxx", "default"} => %Provider{
            contract_id: "wasmcloud:test",
            id: "Vxxx",
            instances: [],
            link_name: "default"
          }
        }
      }

      assert l == desired
      l = Lattice.apply_event(l, stop)
      assert l == desired
      l = Lattice.apply_event(l, start)
      assert l == orig_desired
    end

    test "Stores multiple instances of a provider across hosts" do
      start =
        CloudEvents.provider_started(
          "Vxxx",
          @test_contract,
          "default",
          "abc123",
          @test_spec,
          @test_host
        )

      stamp = Lattice.timestamp_from_iso8601(start.time)
      l = Lattice.apply_event(Lattice.new(), start)

      start2 =
        CloudEvents.provider_started(
          "Vxxx",
          @test_contract,
          "default",
          "abc456",
          @test_spec,
          @test_host
        )

      stamp2 = Lattice.timestamp_from_iso8601(start2.time)
      l = Lattice.apply_event(l, start2)

      assert l == %Lattice{
               actors: %{},
               hosts: %{},
               instance_tracking: %{"abc123" => stamp, "abc456" => stamp2},
               linkdefs: [],
               providers: %{
                 {"Vxxx", "default"} => %Provider{
                   contract_id: "wasmcloud:test",
                   id: "Vxxx",
                   instances: [
                     %Instance{host_id: "Nxxx", id: "abc456", spec_id: "testapp"},
                     %Instance{host_id: "Nxxx", id: "abc123", spec_id: "testapp"}
                   ],
                   link_name: "default"
                 }
               }
             }

      # Add a new instance from a different spec
      start3 =
        CloudEvents.provider_started(
          "Vxxx",
          @test_contract,
          "default",
          "abc789",
          @test_spec_2,
          @test_host
        )

      stamp3 = Lattice.timestamp_from_iso8601(start3.time)

      l = Lattice.apply_event(l, start3)

      assert l == %Lattice{
               actors: %{},
               hosts: %{},
               instance_tracking: %{
                 "abc123" => stamp,
                 "abc456" => stamp2,
                 "abc789" => stamp3
               },
               linkdefs: [],
               providers: %{
                 {"Vxxx", "default"} => %Provider{
                   contract_id: "wasmcloud:test",
                   id: "Vxxx",
                   instances: [
                     %Instance{
                       host_id: "Nxxx",
                       id: "abc789",
                       spec_id: "othertestapp"
                     },
                     %Instance{
                       host_id: "Nxxx",
                       id: "abc456",
                       spec_id: "testapp"
                     },
                     %Instance{
                       host_id: "Nxxx",
                       id: "abc123",
                       spec_id: "testapp"
                     }
                   ],
                   link_name: "default"
                 }
               }
             }
    end
  end
end
