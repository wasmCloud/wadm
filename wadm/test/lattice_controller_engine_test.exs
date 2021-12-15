defmodule WadmTest do
  use ExUnit.Case
  use Supervisor

  require Logger

  test "test hydrate state" do
    {:ok, sup} =  Wadm.Supervisor.start_link([]) ## should be start supervised Gnat.ConnectionSupervisor
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect :sys.get_state(sup)}")

    {:ok, lattice} = LatticeObserver.NatsObserver.start_link(
    %{
        supervised_connection: :lattice_connection_supervisor,
        module: Wadm.LatticeWatcher,
        lattice_prefix: "default"
    })

    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect :sys.get_state(lattice)}")

    assert LatticeObserver.NatsObserver.get_hosts(lattice) == []
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect GenServer.whereis(:gnat)}")

  end

  test "api test no connection to supervise" do
    registry =
      start_supervised!({
        LatticeObserver.NatsObserver,
        %{supervised_connection: false, lattice_prefix: "default"}
      })

    assert LatticeObserver.NatsObserver.get_prefix(registry) == "default"
    assert LatticeObserver.NatsObserver.get_hosts(registry) == []

    assert LatticeObserver.NatsObserver.get_observed_lattice(registry) ==
             %LatticeObserver.Observed.Lattice{
               actors: %{},
               hosts: %{},
               instance_tracking: %{},
               linkdefs: [],
               parameters: %LatticeObserver.Observed.Lattice.Parameters{
                 host_status_decay_rate_seconds: 35
               },
               providers: %{},
               refmap: %{}
             }
  end
end
