defmodule BotArmySynapse.ArmyContext do
  @moduledoc """
  Publishes shared context to the bot army via `army.context` NATS subject.

  Synapse is the integrator that broadcasts:
  - Goal health and status (from GoalStore)
  - User context (mode, energy, focus)
  - Army ethics (refusals, priorities, spend limits)

  This is the single source of truth for what is happening *right now* across the army.
  All bots subscribe to `army.context` to make decisions.

  ## Message Format

  ```json
  {
    "timestamp": "2026-04-25T14:30:00Z",
    "source": "bot_army_synapse",
    "goals": {
      "total": 8,
      "active": 5,
      "at_risk": 2,
      "last_sync": "2026-04-25T14:30:00Z",
      "sync_failures": 0,
      "at_risk_goals": [
        {
          "id": "goal-1",
          "name": "Synapse Enhancement",
          "status": "active",
          "last_decision_at": "2026-04-18T10:00:00Z",
          "decision_count": 3
        }
      ]
    },
    "user": {
      "mode": "focused",
      "energy": "high",
      "current_focus": "system-work"
    }
  }
  ```
  """

  require Logger

  @doc """
  Publish current goal health and army state to `army.context`.

  Called by GoalStore after each sync to keep the army informed of goal status.
  """
  def publish_goal_context(goals_map) when is_map(goals_map) do
    try do
      with {:ok, conn} <-
             GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 1000) do
        at_risk_goals = filter_at_risk_goals(goals_map)

        context_payload = %{
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
          "source" => "bot_army_synapse",
          "goals" => %{
            "total" => map_size(goals_map),
            "active" => count_active_goals(goals_map),
            "at_risk" => length(at_risk_goals),
            "at_risk_goals" => format_at_risk_goals(at_risk_goals),
            "last_sync" => DateTime.utc_now() |> DateTime.to_iso8601()
          }
        }

        Gnat.pub(conn, "army.context", Jason.encode!(context_payload))
        Logger.debug("[ArmyContext] Published goal context to army.context")
        :ok
      end
    rescue
      e ->
        Logger.warning("[ArmyContext] Failed to publish goal context: #{inspect(e)}")
        :ok
    end
  end

  def publish_goal_context(_), do: :ok

  # Private helpers

  defp filter_at_risk_goals(goals_map) do
    goals_map
    |> Map.values()
    |> Enum.filter(&goal_at_risk?/1)
  end

  defp goal_at_risk?(goal) when is_map(goal) do
    last_decision_at = Map.get(goal, :last_decision_at)
    decision_count = Map.get(goal, :decision_count, 0)

    if is_nil(last_decision_at) or decision_count == 0 do
      true
    else
      case DateTime.from_iso8601(last_decision_at) do
        {:ok, dt, _} ->
          days_since = DateTime.diff(DateTime.utc_now(), dt, :day)
          days_since >= 7

        _ ->
          false
      end
    end
  end

  defp goal_at_risk?(_), do: false

  defp count_active_goals(goals_map) do
    goals_map
    |> Map.values()
    |> Enum.count(&(&1["status"] == "active"))
  end

  defp format_at_risk_goals(at_risk_goals) do
    Enum.map(at_risk_goals, fn goal ->
      %{
        "id" => goal["id"],
        "name" => goal["name"],
        "status" => goal["status"],
        "area" => goal["area"] || "general",
        "last_decision_at" => Map.get(goal, :last_decision_at),
        "decision_count" => Map.get(goal, :decision_count, 0)
      }
    end)
  end
end
