defmodule LatticeControllerEngineTest.Reconciler.LinkdefsTest do
  use ExUnit.Case

  alias LatticeControllerEngine.Observed.{Lattice, Instance}
  alias LatticeControllerEngine.Reconciler

  alias LatticeControllerEngine.Model.{
    AppSpec,
    LinkDefinition,
    ActorComponent,
    CapabilityComponent
  }

  @single_ld_spec AppSpec.new("test", "1.0", "this is a test", [
                    %ActorComponent{
                      name: "testactor1",
                      image: "testregistry.org/testactor:0.0.1",
                      traits: [
                        %LinkDefinition{
                          target: "testprovider1",
                          values: %{"foo" => "bar"}
                        }
                      ]
                    },
                    %CapabilityComponent{
                      name: "testprovider1",
                      image: "testregistry.org/testprovider:0.0.1",
                      contract: "wasmcloud:testing",
                      link_name: "default",
                      traits: []
                    }
                  ])

  describe "Reconciler properly handles checking desired link definitions" do
    test "Reconciler fails to recommend linkdef put when keys cannot be looked up from OCI ref" do
      lattice = Lattice.new()

      assert Reconciler.AppSpec.reconcile(@single_ld_spec, lattice) == [
               %LatticeControllerEngine.Reconciler.Command{
                 cmd: :no_action,
                 params: %{},
                 reason:
                   "Link definition target 'testprovider1' or source 'testactor1' may not be running in the lattice"
               }
             ]
    end

    test "Reconciler fails to recommend linkdef put when keys cannot be looked up from OCI ref (partial map)" do
      lattice = %Lattice{
        Lattice.new()
        | ocimap: %{
            "testregistry.org/testactor:0.0.1" => "Mxxx"
          }
      }

      assert Reconciler.AppSpec.reconcile(@single_ld_spec, lattice) == [
               %LatticeControllerEngine.Reconciler.Command{
                 cmd: :no_action,
                 params: %{},
                 reason:
                   "Link definition target 'testprovider1' or source 'testactor1' may not be running in the lattice"
               }
             ]
    end

    test "Reconciler does no-op when link definition already exists" do
      lattice = %Lattice{
        Lattice.new()
        | linkdefs: [
            %LatticeControllerEngine.Observed.LinkDefinition{
              actor_id: "Mxxx",
              contract_id: "wasmcloud:testing",
              link_name: "default",
              provider_id: "Vxxx",
              values: %{"foo" => "bar"}
            }
          ],
          ocimap: %{
            "testregistry.org/testactor:0.0.1" => "Mxxx",
            "testregistry.org/testprovider:0.0.1" => "Vxxx"
          }
      }

      assert Reconciler.AppSpec.reconcile(@single_ld_spec, lattice) == [
               %LatticeControllerEngine.Reconciler.Command{
                 cmd: :no_action,
                 params: %{},
                 reason: "Link definition already exists"
               }
             ]
    end

    test "Reconciler recommends put when both target and source have OCI ref maps" do
      lattice = %Lattice{
        Lattice.new()
        | ocimap: %{
            "testregistry.org/testactor:0.0.1" => "Mxxx",
            "testregistry.org/testprovider:0.0.1" => "Vxxx"
          }
      }

      assert Reconciler.AppSpec.reconcile(@single_ld_spec, lattice) == [
               %LatticeControllerEngine.Reconciler.Command{
                 cmd: :put_linkdef,
                 params: %{
                   ld: %LatticeControllerEngine.Observed.LinkDefinition{
                     actor_id: "Mxxx",
                     contract_id: "wasmcloud:testing",
                     link_name: "default",
                     provider_id: "Vxxx",
                     values: %{"foo" => "bar"}
                   }
                 },
                 reason: ""
               }
             ]
    end
  end
end
