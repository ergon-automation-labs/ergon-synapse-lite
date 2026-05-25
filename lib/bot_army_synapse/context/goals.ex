defmodule BotArmySynapse.Context.Goals do
  @moduledoc """
  Goals context handler.

  Reads active goals from GoalStore directly (zero-latency, no NATS call).
  Handles GTD project events to trigger cache refresh.
  """

  require Logger

  @doc "Gather active goals context."
  def get_context do
    result = BotArmySynapse.GoalStore.list_goals(:all)
    Logger.debug("[Goals.get_context] Returned: #{inspect(result, limit: 500)}")
    result
  rescue
    e ->
      Logger.warning("[Goals.get_context] Exception: #{inspect(e)}")
      nil
  end

  @doc "Handle GTD project events to refresh cache."
  def handle_event(_message, _event) do
    BotArmySynapse.GoalStore.refresh()
    :ok
  end
end
