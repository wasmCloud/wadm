defmodule WadmTest.Model.YamlJsonTest do
  @simple_path "test/fixtures/simple1.yaml"
  @simple_path_json "test/fixtures/simple1.json"

  use ExUnit.Case
  alias Wadm.Model.AppSpec

  describe "Testing YAML decoding" do
    setup do
      [
        simple_yaml: YamlElixir.read_from_file!(@simple_path),
        simple_json: File.read!(@simple_path_json) |> Jason.decode!()
      ]
    end

    test "Decode the simple JSON file", fixture do
      {:ok, app_spec} = AppSpec.from_map(fixture.simple_json)

      assert app_spec.name == "my-example-app"
    end

    test "Decode the simple YAML file", fixture do
      {:ok, app_spec} = AppSpec.from_map(fixture.simple_yaml)

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
                 # we strip the preceding v from version now
                 version: "0.0.1"
               }
    end
  end
end
