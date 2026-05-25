defmodule BotArmySynapse.Repo do
  @moduledoc """
  Ecto repository for Synapse knowledge graph persistence.
  """
  use Ecto.Repo,
    otp_app: :bot_army_synapse,
    adapter: Ecto.Adapters.Postgres
end
