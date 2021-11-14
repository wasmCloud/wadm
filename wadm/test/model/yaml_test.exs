defmodule WadmTest.Model.YamlTest do
  @simple_path "test/fixtures/simple1.yaml"

  use ExUnit.Case
  alias Wadm.Model.AppSpec

  describe "Testing YAML decoding" do
    setup do
      [
        simple_yaml: YamlElixir.read_from_file!(@simple_path)
      ]
    end

    test "Decode the simple YAML file", fixture do
      {:ok, app_spec} = AppSpec.from_yaml(fixture.simple_yaml)

      assert app_spec ==
               %Wadm.Model.AppSpec{
                 components: [
                   %Wadm.Model.ActorComponent{
                     image: "wasmcloud.azurecr.io/fake:1",
                     name: "userinfo",
                     traits: [
                       %Wadm.Model.SpreadScaler{
                         replicas: 4,
                         spread: [
                           %Wadm.Model.WeightedTarget{
                             name: "eastcoast",
                             requirements: %{"zone" => "us-east-1"},
                             weight: 80
                           },
                           %Wadm.Model.WeightedTarget{
                             name: "westcoast",
                             requirements: %{"zone" => "us-west-1"},
                             weight: 20
                           }
                         ]
                       },
                       %Wadm.Model.LinkDefinition{
                         target: "webcap",
                         values: %{"port" => 8080}
                       }
                     ]
                   },
                   %Wadm.Model.CapabilityComponent{
                     contract: "wasmcloud:httpserver",
                     image: "wasmcloud.azurecr.io/httpserver:0.13.1",
                     link_name: "default",
                     name: "webcap",
                     traits: []
                   },
                   %Wadm.Model.CapabilityComponent{
                     contract: "wasmcloud:blinkenlights",
                     image: "wasmcloud.azurecr.io/ledblinky:0.0.1",
                     link_name: "default",
                     name: "ledblinky",
                     traits: [
                       %Wadm.Model.SpreadScaler{
                         replicas: 1,
                         spread: [
                           %Wadm.Model.WeightedTarget{
                             name: "haslights",
                             requirements: %{"ledenabled" => true},
                             weight: 100
                           }
                         ]
                       }
                     ]
                   }
                 ],
                 description: "This is my app",
                 name: "my-example-app",
                 version: "v0.0.1"
               }
    end
  end
end
