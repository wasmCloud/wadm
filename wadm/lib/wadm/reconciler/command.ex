defmodule Wadm.Reconciler.Command do
  alias __MODULE__

  defstruct [:cmd, :reason, :params]

  @type t :: %Command{
          cmd: atom(),
          reason: String.t(),
          params: Map.t()
        }

  @spec new(atom(), String.t(), Map.t()) :: Wadm.Reconciler.Command.t()
  def new(cmd, reason, params) do
    %Command{
      cmd: cmd,
      reason: reason,
      params: params
    }
  end
end
