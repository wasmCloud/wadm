defmodule WadmTest.Model.ValidatorTest do
  @simple_path "test/fixtures/simple1.yaml"
  @validfails_path "test/fixtures/valid_fails.yaml"
  @nospread_path "test/fixtures/no_spreads.yaml"
  @mixedspread_path "test/fixtures/mixed_spreads.yaml"

  use ExUnit.Case
  alias Wadm.Model.{AppSpec, Validator}

  describe "Testing AppSpec Validation" do
    setup do
      {:ok, pass_spec} = AppSpec.from_map(YamlElixir.read_from_file!(@simple_path))
      {:ok, fail_spec} = AppSpec.from_map(YamlElixir.read_from_file!(@validfails_path))
      {:ok, nospreads_spec} = AppSpec.from_map(YamlElixir.read_from_file!(@nospread_path))
      {:ok, mixedspreads_spec} = AppSpec.from_map(YamlElixir.read_from_file!(@mixedspread_path))

      [
        pass_spec: pass_spec,
        fail_spec: fail_spec,
        nospreads_spec: nospreads_spec,
        mixedspreads_spec: mixedspreads_spec
      ]
    end

    test "Valid spec passes validation", fixture do
      assert Validator.validate_appspec(fixture.pass_spec) == :ok
    end

    test "No spread definition spec passes validation", fixture do
      assert Validator.validate_appspec(fixture.nospreads_spec) == :ok
    end

    test "Mixed spread definition spec passes validation", fixture do
      assert Validator.validate_appspec(fixture.mixedspreads_spec) == :ok
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
