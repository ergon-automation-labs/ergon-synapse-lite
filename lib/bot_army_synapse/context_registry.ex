defmodule BotArmySynapse.ContextRegistry do
  @moduledoc """
  Registry for context source modules.

  Each context source implements `get_context/0` and optionally `handle_event/1`.
  Register sources in config:

      config :bot_army_synapse, :context_modules, %{
        "gtd" => BotArmySynapse.Context.GTD,
        "calendar" => BotArmySynapse.Context.Calendar,
        "fleet" => BotArmySynapse.Context.Fleet
      }
  """

  @spec get(String.t()) :: module() | nil
  def get(source_name) do
    sources() |> Map.get(source_name)
  end

  @spec sources() :: %{String.t() => module()}
  def sources do
    Application.get_env(:bot_army_synapse, :context_modules, %{})
  end

  @spec source_names() :: [String.t()]
  def source_names do
    sources() |> Map.keys() |> Enum.sort()
  end
end
