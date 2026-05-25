defmodule BotArmySynapse.Release do
  @moduledoc """
  Release tasks for Synapse.

  Migrations are run via the shared BotArmyRuntime.Ecto.MigrationRunner:

      /path/to/synapse/bin/synapse eval 'BotArmySynapse.Release.migrate()'

  Called from Salt during bot deployment, before the bot starts.
  """

  alias BotArmyRuntime.Ecto.MigrationRunner

  @app :bot_army_synapse

  def migrate do
    MigrationRunner.run(
      repo_module: BotArmySynapse.Repo,
      app_module: @app
    )
  end
end
