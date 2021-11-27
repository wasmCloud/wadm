defmodule Wadm.MixProject do
  use Mix.Project

  def project do
    [
      app: :wadm,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: compiler_paths(Mix.env()),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Wadm.Application, []}
    ]
  end

  defp deps do
    [
      {:yaml_elixir, "~> 2.8"},
      {:cloudevents, "~> 0.4.0"},
      {:uuid, "~> 1.1"},
      {:lattice_observer, git: "https://github.com/janitha09/lattice-observer"}
    ]
  end

  def compiler_paths(:test), do: ["lib", "test/support"]
  def compiler_paths(_), do: ["lib"]
end
