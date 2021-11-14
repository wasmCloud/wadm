defmodule WadmTest.Model.ValidatorTest do
  @simple_path "test/fixtures/simple1.yaml"
  @validfails_path "test/fixtures/valid_fails.yaml"

  use ExUnit.Case
  alias Wadm.Model.{AppSpec, Validator}

  describe "Testing AppSpec Validation" do
    setup do
      {:ok, pass_spec} = AppSpec.from_yaml(YamlElixir.read_from_file!(@simple_path))
      {:ok, fail_spec} = AppSpec.from_yaml(YamlElixir.read_from_file!(@validfails_path))

      [
        pass_spec: pass_spec,
        fail_spec: fail_spec
      ]
    end

    test "Valid spec passes validation", fixture do
      assert Validator.validate_appspec(fixture.pass_spec) == :ok
    end

    test "Invalid spec fails validation", fixture do
      assert Validator.validate_appspec(fixture.fail_spec) ==
               {:error,
                [
                  "No matching targets found for link definition target webcarp",
                  "Spread scaler weight does not add up to 100"
                ]}
    end
  end
end
