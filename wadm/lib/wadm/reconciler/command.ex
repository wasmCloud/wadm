defmodule Wadm.Reconciler.Command do
  alias __MODULE__
  @appspec "wasmcloud.dev/appspec"

  defstruct [:cmd, :reason, :params]

  @type command_type() :: :no_action | :start | :stop | :put_linkdef | :error

  @type t :: %Command{
          cmd: command_type(),
          reason: String.t(),
          params: Map.t()
        }

  @spec new(command_type(), String.t(), Map.t()) :: Wadm.Reconciler.Command.t()
  def new(cmd, reason, params) do
    %Command{
      cmd: cmd,
      reason: reason,
      params: params
    }
  end

  # Converts the :put_linkdef command into a request on the linkdefs.put topic
  def to_lattice_control_command(
        _spec,
        lattice_id,
        %Command{
          cmd: :put_linkdef,
          params: %{
            ld: linkdef = %LatticeObserver.Observed.LinkDefinition{}
          }
        }
      ) do
    {
      "wasmbus.ctl.#{lattice_id}.linkdefs.put",
      %{
        actor_id: linkdef.actor_id,
        provider_id: linkdef.provider_id,
        values: linkdef.values,
        contract_id: linkdef.contract_id,
        link_name: linkdef.link_name
      }
      |> encode()
    }
  end

  # Converts a :start command for a provider into a `lp` command
  # Note the more specific provider starts come BEFORE the less specific
  # actor starts
  def to_lattice_control_command(
        spec,
        lattice_id,
        %Command{
          cmd: :start,
          params: %{
            host_id: host_id,
            image: image,
            link_name: link_name,
            contract_id: contract_id
          },
          reason: _reason
        }
      ) do
    {
      "wasmbus.ctl.#{lattice_id}.cmd.#{host_id}.lp",
      %{
        host_id: host_id,
        provider_ref: image,
        link_name: link_name,
        contract_id: contract_id,
        annotations: spec |> extract_annotations()
      }
      |> encode()
    }
  end

  # Converts a :start command for an actor into a `la` command
  def to_lattice_control_command(
        spec,
        lattice_id,
        %Command{
          cmd: :start,
          params: %{host_id: host_id, image: image},
          reason: _reason
        }
      ) do
    {
      "wasmbus.ctl.#{lattice_id}.cmd.#{host_id}.la",
      %{
        count: 1,
        actor_ref: image,
        host_id: host_id,
        annotations: spec |> extract_annotations()
      }
      |> encode()
    }
  end

  # Converts a :stop command for a provider into an `sp` command
  def to_lattice_control_command(
        _spec,
        lattice_id,
        %Command{
          cmd: :stop,
          params: %{
            host_id: host_id,
            id: id,
            link_name: link_name
          },
          reason: _reason
        }
      ) do
    {
      "wasmbus.ctl.#{lattice_id}.cmd.#{host_id}.sp",
      %{
        host_id: host_id,
        provider_ref: id,
        link_name: link_name
      }
      |> encode()
    }
  end

  # Converts a :stop command for an actor into a `sa` command
  def to_lattice_control_command(
        spec,
        lattice_id,
        %Command{
          cmd: :stop,
          params: %{
            host_id: host_id,
            image: _image,
            id: id
          },
          reason: _reason
        }
      ) do
    {
      "wasmbus.ctl.#{lattice_id}.cmd.#{host_id}.sa",
      %{
        actor_ref: id,
        count: 1,
        annotations: spec |> extract_annotations()
      }
      |> encode()
    }
  end

  defp extract_annotations(spec) do
    %{
      @appspec => spec.name
    }
  end

  defp encode(raw) do
    raw |> Jason.encode!() |> IO.iodata_to_binary()
  end
end
