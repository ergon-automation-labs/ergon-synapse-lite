defmodule BotArmySynapse.MixProject do
  use Mix.Project

  def project do
    [
      app: :bot_army_synapse,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: [
        synapse: [
          applications: [bot_army_synapse: :permanent]
        ]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :jason],
      mod: {BotArmySynapse.Application, []}
    ]
  end

  defp deps do
    [
      {:bot_army_library_core,
       git: "https://github.com/ergon-automation-labs/ergon-library-core.git", branch: "main"},
      {:bot_army_library_runtime,
       git: "https://github.com/ergon-automation-labs/ergon-library-runtime.git", branch: "main"},
      {:bot_army_library_learning,
       git: "https://github.com/ergon-automation-labs/ergon-library-learning.git", branch: "main"},
      {:ecto_sql, "~> 3.10"},
      {:postgrex, "~> 0.17"},
      {:jason, "~> 1.4"},
      {:logger_json, "~> 5.1"},
      {:elixir_uuid, "~> 1.2"},
      {:tz, "~> 0.12"},
      # NATS client for consuming bot events
      {:gnat, "~> 1.2"},

      # Development/Test
      {:ex_doc, "~> 0.30", only: :dev},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.17", only: :test}
    ]
  end
end
