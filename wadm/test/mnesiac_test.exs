defmodule WadmTest.Mnesaic do
  use ExUnit.ClusteredCase, async: false
  require Mnesiac
  require Logger

  @single_unnamed_opts [
    boot_timeout: 10_000,
    nodes: [
      [
        name: :"test01@127.0.0.1",
        config: [
          mnesia: [dir: Kernel.to_charlist(Path.join(File.cwd!(), "test01"))],
          mnesiac: [
            stores: [Wadm.LatticeStore],
            schema_type: :disc_copies,
            table_load_timeout: 600_000
          ]
        ]
      ]
    ]
  ]

  @distributed_opts [
    boot_timeout: 10_000,
    nodes: [
      [
        name: :"test03@127.0.0.1",
        config: [
          mnesia: [dir: to_charlist(Path.join(File.cwd!(), "test03"))],
          mnesiac: [
            stores: [Wadm.LatticeStore],
            schema_type: :disc_copies,
            table_load_timeout: 600_000
          ]
        ]
      ],
      [
        name: :"test04@127.0.0.1",
        config: [
          mnesia: [dir: to_charlist(Path.join(File.cwd!(), "test04"))],
          mnesiac: [
            stores: [Wadm.LatticeStore],
            schema_type: :disc_copies,
            table_load_timeout: 600_000
          ]
        ]
      ]
    ]
  ]

  scenario "single node test with mnesiac supervisor/1", @single_unnamed_opts do
    node_setup do
      Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(node())}")
      {:ok, _pid} = Mnesiac.Supervisor.start_link([[node()]])
      :ok = :mnesia.wait_for_tables([Wadm.LatticeStore], 5000)
    end

    test "tables exist", %{cluster: cluster} do
      assert length(Cluster.members(cluster)) == 1
      [node_a] = Cluster.members(cluster)
      assert node_a == :"test01@127.0.0.1"

      tables = Cluster.call(node_a, :mnesia, :system_info, [:tables])

      assert true = Cluster.call(node_a, Enum, :member?, [tables, :schema])
      assert true = Cluster.call(node_a, Enum, :member?, [tables, Wadm.LatticeStore])
      assert :opt_disc = Cluster.call(node_a, :mnesia, :system_info, [:schema_location])

      assert [{:running_nodes, [node_a]}] = Cluster.call(node_a, Mnesiac, :cluster_status, [])
      assert true = Cluster.call(node_a, Mnesiac, :running_db_node?, [node_a])

      key = "wadmcache:default"
      value = :erlang.term_to_binary(LatticeObserver.Observed.Lattice.new())

      assert :ok =
               Cluster.call(node_a, :mnesia, :dirty_write, [
                 {Wadm.LatticeStore, "id_1", key, value}
               ])

      assert [{Wadm.LatticeStore, "id_1", key, value}] =
               Cluster.call(node_a, :mnesia, :dirty_read, [{Wadm.LatticeStore, "id_1"}])
    end
  end

  scenario "distributed test", @distributed_opts do
    node_setup do
      {:ok, _pid} = Mnesiac.Supervisor.start_link([[:"test03@127.0.0.1", :"test04@127.0.0.1"]])

      if node() == :"test03@127.0.0.1" do
        :ok = :mnesia.wait_for_tables([Mnesia.Support.ExampleStore], 5000)
      end

      if node() == :"test04@127.0.0.1" do
        :ok = :mnesia.wait_for_tables([Wadm.LatticeStore], 10_000)
      end
    end

    test "tables exist and replicatiing", %{cluster: cluster} do
      assert length(Cluster.members(cluster)) == 2
      [node_a, node_b] = Cluster.members(cluster)

      tables_a = Cluster.call(node_a, :mnesia, :system_info, [:tables])
      tables_b = Cluster.call(node_b, :mnesia, :system_info, [:tables])

      assert true = Cluster.call(node_a, Enum, :member?, [tables_a, :schema])
      assert true = Cluster.call(node_b, Enum, :member?, [tables_b, :schema])
      assert true = Cluster.call(node_a, Enum, :member?, [tables_a, Wadm.LatticeStore])
      assert true = Cluster.call(node_b, Enum, :member?, [tables_b, Wadm.LatticeStore])
      assert :opt_disc = Cluster.call(node_a, :mnesia, :system_info, [:schema_location])
      assert :opt_disc = Cluster.call(node_b, :mnesia, :system_info, [:schema_location])

      assert [{:running_nodes, [node_a, node_b]}] =
               Cluster.call(node_a, Mnesiac, :cluster_status, [])

      assert [{:running_nodes, [node_a, node_b]}] =
               Cluster.call(node_b, Mnesiac, :cluster_status, [])

      assert [node_a, node_b] = Cluster.call(node_a, Mnesiac, :running_nodes, [])
      assert [node_a, node_b] = Cluster.call(node_b, Mnesiac, :running_nodes, [])

      assert true = Cluster.call(node_a, Mnesiac, :node_in_cluster?, [node_b])
      assert true = Cluster.call(node_b, Mnesiac, :node_in_cluster?, [node_a])

      assert true = Cluster.call(node_a, Mnesiac, :running_db_node?, [node_b])
      assert true = Cluster.call(node_b, Mnesiac, :running_db_node?, [node_a])

      key = "wadmcache:default"
      value = :erlang.term_to_binary(LatticeObserver.Observed.Lattice.new())

      assert :ok =
               Cluster.call(node_a, :mnesia, :dirty_write, [
                 {Wadm.LatticeStore, "id_1", key, value}
               ])

      assert [{Wadm.LatticeStore, "id_1", key, value}] =
               Cluster.call(node_b, :mnesia, :dirty_read, [{Wadm.LatticeStore, "id_1"}])
    end
  end
end
