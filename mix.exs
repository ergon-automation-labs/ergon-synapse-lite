defmodule BotArmySynapse.MixProject do
  use Mix.Project

  def project do
    [
      app: :bot_army_synapse,
      version: "0.1.2",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      default_release: :synapse_bot,
      releases: [
        synapse_bot: [
          applications: [bot_army_synapse: :permanent]
        ],
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
      {:bot_army_library_core, path: "../bot_army_library_core", override: true},
      {:bot_army_library_runtime, path: "../bot_army_library_runtime", override: true},
      {:bot_army_library_learning, path: "../bot_army_library_learning", override: true},
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
