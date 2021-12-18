defmodule Wadm.Application do
  use Application

  def start(_type, _args) do
    Wadm.Supervisor.start_link(name: Wadm.Supervisor)
  end
end
