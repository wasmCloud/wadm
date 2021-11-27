defmodule WadmTest.Observed.ActorsTest do
  use ExUnit.Case
  alias LatticeObserver.Observed.{Lattice, Instance}
  alias TestSupport.CloudEvents

  @test_spec "testapp"
  @test_spec_2 "othertestapp"
  @test_host "Nxxx"

  describe "Observed Lattice Monitors Actor Events" do
    test "Adds and Removes actors" do
      start = CloudEvents.actor_started("Mxxx", "abc123", @test_spec, @test_host)
      l = Lattice.new()
      l = Lattice.apply_event(l, start)
      stamp1 = Lattice.timestamp_from_iso8601(start.time)
      # ensure idempotence
      l = Lattice.apply_event(l, start)

      assert l == %Lattice{
               actors: %{
                 "Mxxx" => [
                   %Instance{
                     host_id: "Nxxx",
                     id: "abc123",
                     spec_id: "testapp"
                   }
                 ]
               },
               ocimap: %{},
               hosts: %{},
               instance_tracking: %{
                 "abc123" => stamp1
               },
               linkdefs: [],
               providers: %{}
             }

      stop = CloudEvents.actor_stopped("Mxxx", "abc123", @test_spec, @test_host)
      l = Lattice.apply_event(l, stop)
      # ensure idempotence
      l = Lattice.apply_event(l, stop)

      assert l == %Lattice{
               actors: %{"Mxxx" => []},
               hosts: %{},
               linkdefs: [],
               ocimap: %{},
               providers: %{},
               instance_tracking: %{}
             }
    end

    test "Stores the same actor belonging to multiple specs" do
      start = CloudEvents.actor_started("Mxxx", "abc123", @test_spec, @test_host)
      l = Lattice.new()
      l = Lattice.apply_event(l, start)
      start2 = CloudEvents.actor_started("Mxxx", "abc345", @test_spec_2, @test_host)
      l = Lattice.apply_event(l, start2)
      stamp1 = Lattice.timestamp_from_iso8601(start.time)
      stamp2 = Lattice.timestamp_from_iso8601(start2.time)

      assert l == %Lattice{
               actors: %{
                 "Mxxx" => [
                   %Instance{host_id: "Nxxx", id: "abc345", spec_id: "othertestapp"},
                   %Instance{host_id: "Nxxx", id: "abc123", spec_id: "testapp"}
                 ]
               },
               hosts: %{},
               ocimap: %{},
               instance_tracking: %{
                 "abc123" => stamp1,
                 "abc345" => stamp2
               },
               linkdefs: [],
               providers: %{}
             }

      assert Lattice.actors_in_appspec(l, "testapp") == [
               %{actor_id: "Mxxx", host_id: "Nxxx", instance_id: "abc123"}
             ]

      stop = CloudEvents.actor_stopped("Mxxx", "abc123", @test_spec, @test_host)
      l = Lattice.apply_event(l, stop)

      assert l == %Lattice{
               actors: %{
                 "Mxxx" => [
                   %Instance{
                     host_id: "Nxxx",
                     id: "abc345",
                     spec_id: "othertestapp"
                   }
                 ]
               },
               hosts: %{},
               ocimap: %{},
               instance_tracking: %{
                 "abc345" => stamp2
               },
               linkdefs: [],
               providers: %{}
             }
    end
  end
end
