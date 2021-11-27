defmodule WadmTest.Reconciler.ScalerTest do
  use ExUnit.Case

  alias LatticeObserver.Observed.{Lattice, Instance, Actor, Provider}
  alias Wadm.Reconciler

  alias Wadm.Model.{
    AppSpec,
    LinkDefinition,
    ActorComponent,
    CapabilityComponent,
    SpreadScaler,
    WeightedTarget
  }

  @single_actor_spec AppSpec.new("test", "1.0", "this is a test", [
                       %ActorComponent{
                         name: "testactor1",
                         image: "testregistry.org/testactor:0.0.1",
                         traits: [
                           %SpreadScaler{
                             replicas: 4,
                             spread: [
                               %WeightedTarget{
                                 name: "coolhosts",
                                 weight: 50,
                                 requirements: %{
                                   "style" => "cool"
                                 }
                               },
                               %WeightedTarget{
                                 name: "lamehosts",
                                 weight: 50,
                                 requirements: %{
                                   "style" => "lame"
                                 }
                               }
                             ]
                           }
                         ]
                       }
                     ])

  @single_provider_spec AppSpec.new("test", "1.0", "this is a test", [
                          %CapabilityComponent{
                            name: "testprovider1",
                            link_name: "default",
                            image: "testregistry.org/testprovider:0.0.1",
                            traits: [
                              %SpreadScaler{
                                replicas: 4,
                                spread: [
                                  %WeightedTarget{
                                    name: "coolhosts",
                                    weight: 50,
                                    requirements: %{
                                      "style" => "cool"
                                    }
                                  },
                                  %WeightedTarget{
                                    name: "lamehosts",
                                    weight: 50,
                                    requirements: %{
                                      "style" => "lame"
                                    }
                                  }
                                ]
                              }
                            ]
                          }
                        ])

  describe "Spread Scaler Trait" do
    test "Starts actors when not running yet" do
      lattice = %Lattice{
        Lattice.new()
        | hosts: %{
            "N00001" => %{
              id: "N00001",
              labels: %{
                "style" => "cool",
                "extraneous" => "yup"
              },
              last_seen: DateTime.utc_now()
            },
            "N00002" => %{
              id: "N00002",
              labels: %{
                "style" => "cool"
              },
              last_seen: DateTime.utc_now()
            },
            "N00003" => %{
              id: "N00003",
              labels: %{
                "style" => "lame",
                "extraneous" => "yup"
              },
              last_seen: DateTime.utc_now()
            },
            "N00004" => %{
              id: "N00004",
              labels: %{
                "style" => "lame"
              },
              last_seen: DateTime.utc_now()
            }
          }
      }

      assert Reconciler.AppSpec.reconcile(@single_actor_spec, lattice) ==
               [
                 %Wadm.Reconciler.Command{
                   cmd: :start,
                   params: %{host_id: "N00002", image: "testregistry.org/testactor:0.0.1"},
                   reason: "Weighted target 'coolhosts' needs more instances."
                 },
                 %Wadm.Reconciler.Command{
                   cmd: :start,
                   params: %{host_id: "N00004", image: "testregistry.org/testactor:0.0.1"},
                   reason: "Weighted target 'lamehosts' needs more instances."
                 }
               ]
    end

    test "Adds 1 when weight is not sufficient" do
      lattice = %Lattice{
        Lattice.new()
        | ocimap: %{
            "testregistry.org/testactor:0.0.1" => "M00001"
          },
          actors: %{
            "M00001" => [
              %Instance{
                id: "iid-1",
                host_id: "N00001",
                spec_id: "test"
              },
              %Instance{
                id: "iid-2",
                host_id: "N00002",
                spec_id: "test"
              },
              %Instance{
                id: "iid-3",
                host_id: "N00003",
                spec_id: "test"
              }
            ]
          },
          hosts: %{
            "N00001" => %{
              id: "N00001",
              labels: %{
                "style" => "cool",
                "extraneous" => "yup"
              },
              last_seen: DateTime.utc_now()
            },
            "N00002" => %{
              id: "N00002",
              labels: %{
                "style" => "cool"
              },
              last_seen: DateTime.utc_now()
            },
            "N00003" => %{
              id: "N00003",
              labels: %{
                "style" => "lame"
              },
              last_seen: DateTime.utc_now()
            },
            "N00004" => %{
              id: "N00004",
              labels: %{
                "style" => "lame",
                "extraneous" => "yup"
              },
              last_seen: DateTime.utc_now()
            }
          }
      }

      assert Reconciler.AppSpec.reconcile(@single_actor_spec, lattice) ==
               [
                 %Wadm.Reconciler.Command{
                   cmd: :no_action,
                   params: %{},
                   reason: "Weighted target 'coolhosts' is satisfied"
                 },
                 %Wadm.Reconciler.Command{
                   cmd: :start,
                   params: %{host_id: "N00004", image: "testregistry.org/testactor:0.0.1"},
                   reason: "Weighted target 'lamehosts' needs more instances."
                 }
               ]
    end

    test "Reports failure when insufficient matching hosts to reconcile" do
      lattice = %Lattice{
        Lattice.new()
        | ocimap: %{
            "testregistry.org/testprovider:0.0.1" => "V00001"
          },
          providers: %{
            {"V00001", "default"} => %Provider{
              id: "V00001",
              link_name: "default",
              contract_id: "wasmcloud:test",
              instances: [
                %Instance{
                  id: "iid-1",
                  host_id: "N00001",
                  spec_id: "test"
                },
                %Instance{
                  id: "iid-2",
                  host_id: "N00002",
                  spec_id: "test"
                },
                %Instance{
                  id: "iid-3",
                  host_id: "N00003",
                  spec_id: "test"
                }
              ]
            }
          },
          hosts: %{
            "N00001" => %{
              id: "N00001",
              labels: %{
                "style" => "cool"
              },
              last_seen: DateTime.utc_now()
            },
            "N00002" => %{
              id: "N00002",
              labels: %{
                "style" => "cool"
              },
              last_seen: DateTime.utc_now()
            },
            "N00003" => %{
              id: "N00003",
              labels: %{
                "style" => "lame"
              },
              last_seen: DateTime.utc_now()
            },
            "N00004" => %{
              id: "N00004",
              labels: %{
                "style" => "notnearlycoolenough"
              },
              last_seen: DateTime.utc_now()
            }
          }
      }

      assert Reconciler.AppSpec.reconcile(@single_provider_spec, lattice) == [
               %Wadm.Reconciler.Command{
                 cmd: :no_action,
                 params: %{link_name: "default"},
                 reason: "Weighted target 'coolhosts' is satisfied"
               },
               %Wadm.Reconciler.Command{
                 cmd: :error,
                 params: %{link_name: "default"},
                 reason: "Weighted target 'lamehosts' has insufficient candidate hosts."
               }
             ]
    end

    test "Distributes providers evently" do
      lattice = %Lattice{
        Lattice.new()
        | ocimap: %{
            "testregistry.org/testprovider:0.0.1" => "V00001"
          },
          providers: %{
            {"V00001", "default"} => %Provider{
              id: "V00001",
              link_name: "default",
              contract_id: "wasmcloud:test",
              instances: []
            }
          },
          hosts: %{
            "N00001" => %{
              id: "N00001",
              labels: %{
                "style" => "cool"
              },
              last_seen: DateTime.utc_now()
            },
            "N00002" => %{
              id: "N00002",
              labels: %{
                "style" => "lame"
              },
              last_seen: DateTime.utc_now()
            }
          }
      }

      assert Reconciler.AppSpec.reconcile(@single_provider_spec, lattice) == [
               %Wadm.Reconciler.Command{
                 cmd: :start,
                 params: %{
                   host_id: "N00001",
                   image: "testregistry.org/testprovider:0.0.1",
                   link_name: "default"
                 },
                 reason: "Weighted target 'coolhosts' needs more instances."
               },
               %Wadm.Reconciler.Command{
                 cmd: :start,
                 params: %{
                   host_id: "N00002",
                   image: "testregistry.org/testprovider:0.0.1",
                   link_name: "default"
                 },
                 reason: "Weighted target 'lamehosts' needs more instances."
               }
             ]
    end

    test "Recommends stopping instances when more are running than necessary" do
      lattice = %Lattice{
        Lattice.new()
        | ocimap: %{
            "testregistry.org/testactor:0.0.1" => "M00001"
          },
          actors: %{
            "M00001" => [
              %Instance{
                id: "iid-1",
                host_id: "N00001",
                spec_id: "test"
              },
              %Instance{
                id: "iid-4",
                host_id: "N00001",
                spec_id: "test"
              },
              %Instance{
                id: "iid-2",
                host_id: "N00002",
                spec_id: "test"
              },
              %Instance{
                id: "iid-3",
                host_id: "N00003",
                spec_id: "test"
              },
              %Instance{
                id: "iid-5",
                host_id: "N00004",
                spec_id: "test"
              }
            ]
          },
          hosts: %{
            "N00001" => %{
              id: "N00001",
              labels: %{
                "style" => "cool"
              },
              last_seen: DateTime.utc_now()
            },
            "N00002" => %{
              id: "N00002",
              labels: %{
                "style" => "cool"
              },
              last_seen: DateTime.utc_now()
            },
            "N00003" => %{
              id: "N00003",
              labels: %{
                "style" => "lame"
              },
              last_seen: DateTime.utc_now()
            },
            "N00004" => %{
              id: "N00004",
              labels: %{
                "style" => "lame"
              },
              last_seen: DateTime.utc_now()
            }
          }
      }

      assert Reconciler.AppSpec.reconcile(@single_actor_spec, lattice) ==
               [
                 %Wadm.Reconciler.Command{
                   cmd: :stop,
                   params: %{
                     host_id: "N00001",
                     id: "M00001",
                     image: "testregistry.org/testactor:0.0.1"
                   },
                   reason: "Weighted target 'coolhosts' has too many instances."
                 },
                 %Wadm.Reconciler.Command{
                   cmd: :no_action,
                   params: %{},
                   reason: "Weighted target 'lamehosts' is satisfied"
                 }
               ]
    end
  end
end
