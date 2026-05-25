defmodule BotArmySynapse.SuggestionDetector do
  @moduledoc """
  Pure functions for detecting stale goals/tasks and system health issues.
  Reuses staleness predicates from DecisionEngine and existing context modules.
  """

  require Logger

  @doc "Classify all tasks by status and other attributes (inbox, next_action, active, waiting_for, overdue, stale)."
  def classify_tasks(tasks) when is_list(tasks) do
    now = DateTime.utc_now()

    tasks
    |> Enum.reject(&task_completed?/1)
    |> Enum.reduce(
      %{
        inbox: [],
        next_action: [],
        active: [],
        waiting_for: [],
        someday_maybe: [],
        overdue: [],
        stale: []
      },
      fn task, acc ->
        status = Map.get(task, "status")
        due_date_str = Map.get(task, "due_date")
        is_overdue = check_overdue(due_date_str, now)
        is_stale = task_older_than_days?(task, 7)

        bucket =
          case status do
            "inbox" -> :inbox
            "next_action" -> :next_action
            "active" -> :active
            "waiting_for" -> :waiting_for
            "someday_maybe" -> :someday_maybe
            _ -> :active
          end

        updated_bucket = [task | Map.get(acc, bucket, [])]
        acc = Map.put(acc, bucket, updated_bucket)

        acc =
          if is_overdue do
            Map.update(acc, :overdue, [task], &[task | &1])
          else
            acc
          end

        if is_stale do
          Map.update(acc, :stale, [task], &[task | &1])
        else
          acc
        end
      end
    )
    |> Enum.map(fn {k, v} -> {k, Enum.reverse(v)} end)
    |> Enum.into(%{})
  end

  def classify_tasks(nil), do: %{}

  @doc "Detect stale goals (no decision in N+ days or never)."
  def stale_goals(goals, stale_days) when is_list(goals) and is_integer(stale_days) do
    goals
    |> Enum.filter(&goal_is_stale?(&1, stale_days))
    |> Enum.sort_by(&goal_sort_key/1, :desc)
  end

  def stale_goals(nil, _stale_days), do: []

  @doc "Detect stale active/next_action tasks (not updated in N+ days) and inbox tasks (>M hours old)."
  def stale_tasks(tasks, active_stale_days, inbox_stale_hours) when is_list(tasks) do
    {inbox_stale, active_stale} =
      tasks
      |> Enum.filter(&task_active_status?/1)
      |> Enum.reject(&task_completed?/1)
      |> Enum.split_with(&task_is_inbox?/1)

    inbox_filtered = Enum.filter(inbox_stale, &task_older_than_hours?(&1, inbox_stale_hours))
    active_filtered = Enum.filter(active_stale, &task_older_than_days?(&1, active_stale_days))

    %{
      inbox: inbox_filtered,
      active: active_filtered
    }
  end

  def stale_tasks(nil, _active_days, _inbox_hours) do
    %{inbox: [], active: []}
  end

  @doc "Check system health: flag bots with degraded health or stale pulses."
  def system_health_issues(known_bots) when is_list(known_bots) do
    known_bots
    |> Enum.filter(&bot_is_degraded?/1)
    |> Enum.map(&health_issue_summary/1)
  end

  def system_health_issues(nil), do: []

  # Private helpers

  defp goal_sort_key(goal) when is_map(goal) do
    iso_str = Map.get(goal, "updated_at", "1970-01-01T00:00:00Z")

    case DateTime.from_iso8601(iso_str) do
      {:ok, dt, _} -> DateTime.to_unix(dt)
      _ -> 0
    end
  end

  defp goal_is_stale?(goal, stale_days) when is_map(goal) do
    case Map.get(goal, :last_decision_at) do
      nil ->
        true

      dt when is_struct(dt, DateTime) ->
        DateTime.diff(DateTime.utc_now(), dt, :day) >= stale_days

      _other ->
        false
    end
  end

  defp check_overdue(due_date_str, now) when is_binary(due_date_str) do
    case Date.from_iso8601(due_date_str) do
      {:ok, due_date} ->
        Date.compare(due_date, DateTime.to_date(now)) == :lt

      _ ->
        false
    end
  end

  defp check_overdue(_due_date, _now), do: false

  defp task_is_inbox?(task) when is_map(task) do
    Map.get(task, "status") == "inbox"
  end

  defp task_active_status?(task) when is_map(task) do
    status = Map.get(task, "status")
    status in ["inbox", "active", "next_action"]
  end

  defp task_completed?(task) when is_map(task) do
    Map.get(task, "status") in ["completed", "deleted"]
  end

  defp task_older_than_hours?(task, hours) when is_map(task) do
    created = Map.get(task, "created_at")

    case DateTime.from_iso8601(created) do
      {:ok, dt, _} ->
        DateTime.diff(DateTime.utc_now(), dt, :second) >= hours * 3600

      _ ->
        false
    end
  end

  defp task_older_than_days?(task, days) when is_map(task) do
    updated = Map.get(task, "updated_at") || Map.get(task, "created_at")

    case DateTime.from_iso8601(updated) do
      {:ok, dt, _} ->
        DateTime.diff(DateTime.utc_now(), dt, :day) >= days

      _ ->
        false
    end
  end

  defp bot_is_degraded?(bot) when is_map(bot) do
    health = Map.get(bot, :health)
    last_pulse = Map.get(bot, :last_pulse_at)

    case health do
      "nominal" ->
        if is_struct(last_pulse, DateTime) do
          DateTime.diff(DateTime.utc_now(), last_pulse, :second) > 10 * 60
        else
          true
        end

      _other ->
        true
    end
  end

  defp health_issue_summary(bot) when is_map(bot) do
    bot_name = Map.get(bot, :name, "unknown")
    health = Map.get(bot, :health, "unknown")
    last_pulse = Map.get(bot, :last_pulse_at)

    age_str =
      if is_struct(last_pulse, DateTime) do
        age_min = DateTime.diff(DateTime.utc_now(), last_pulse, :second) / 60
        " (pulse #{trunc(age_min)}min ago)"
      else
        " (no pulse)"
      end

    %{
      bot_name: bot_name,
      status: health,
      description: "#{bot_name}: #{health}#{age_str}"
    }
  end
end
