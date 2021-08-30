defmodule LatticeControllerEngineTest.Model.YamlTest do
  @simple_path "test/fixtures/simple1.yaml"

  use ExUnit.Case
  alias LatticeControllerEngine.Model.AppSpec

  describe "Testing YAML decoding" do
    setup do
      [
        simple_yaml: YamlElixir.read_from_file!(@simple_path)
      ]
    end

    test "Decode the simple YAML file", fixture do
      {:ok, app_spec} = AppSpec.from_yaml(fixture.simple_yaml)

      assert app_spec ==
               %LatticeControllerEngine.Model.AppSpec{
                 components: [
                   %LatticeControllerEngine.Model.ActorComponent{
                     image: "wasmcloud.azurecr.io/fake:1",
                     name: "userinfo",
                     traits: [
                       %LatticeControllerEngine.Model.SpreadScaler{
                         replicas: 4,
                         spread: [
                           %LatticeControllerEngine.Model.WeightedTarget{
                             name: "eastcoast",
                             requirements: %{"zone" => "us-east-1"},
                             weight: 80
                           },
                           %LatticeControllerEngine.Model.WeightedTarget{
                             name: "westcoast",
                             requirements: %{"zone" => "us-west-1"},
                             weight: 20
                           }
                         ]
                       },
                       %LatticeControllerEngine.Model.LinkDefinition{
                         target: "webcap",
                         type: "linkdef",
                         values: %{"port" => 8080}
                       }
                     ]
                   },
                   %LatticeControllerEngine.Model.CapabilityComponent{
                     contract: "wasmcloud:httpserver",
                     image: "wasmcloud.azurecr.io/httpserver:0.13.1",
                     link_name: "default",
                     name: "webcap",
                     traits: []
                   },
                   %LatticeControllerEngine.Model.CapabilityComponent{
                     contract: "wasmcloud:blinkenlights",
                     image: "wasmcloud.azurecr.io/ledblinky:0.0.1",
                     link_name: "default",
                     name: "ledblinky",
                     traits: [
                       %LatticeControllerEngine.Model.SpreadScaler{
                         replicas: 1,
                         spread: [
                           %LatticeControllerEngine.Model.WeightedTarget{
                             name: "haslights",
                             requirements: %{"ledenabled" => true},
                             weight: 100
                           }
                         ]
                       }
                     ]
                   }
                 ],
                 description: "v0.0.1",
                 name: "my-example-app",
                 version: "This is my app"
               }
    end
  end
end
