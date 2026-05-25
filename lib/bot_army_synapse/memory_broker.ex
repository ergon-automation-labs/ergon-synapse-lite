defmodule BotArmySynapse.MemoryBroker do
  @moduledoc """
  Per-user timeline memory for the bot army city.

  Subscribes to fleet events and stores compact snapshots in ETS.
  Other bots query via NATS:
    - `memory.user.recall` — last N events for a user
    - `memory.user.summary` — aggregated signals (last workout, open tasks, etc.)

  Table: `:synapse_memory_timeline` (public, named, ordered_set)
  Key: `{tenant_id, user_id, timestamp_ms, event_type}`
  Value: compact payload map
  """

  use GenServer
  require Logger

  @table :synapse_memory_timeline
  @max_entries_per_user 500
  @memory_event_types [
    "fitness.workout.logged",
    "fitness.goal.set",
    "gtd.task.created",
    "gtd.task.completed",
    "chore.task.completed",
    "chore.task.notification",
    "rpg.quest.completed",
    "rpg.progression.awarded"
  ]

  # API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Record an event into the timeline.
  """
  @spec record_event(String.t(), String.t() | nil, String.t(), map()) :: :ok
  def record_event(tenant_id, user_id, event_type, payload) when is_map(payload) do
    GenServer.cast(__MODULE__, {:record_event, tenant_id, user_id, event_type, payload})
  end

  @doc """
  Recall last N events for a user.
  """
  @spec recall(String.t(), String.t() | nil, integer()) :: [map()]
  def recall(tenant_id, user_id, limit \\ 20) do
    tid = :ets.whereis(@table)

    if is_reference(tid) do
      pattern = {{tenant_id, user_id || :_, :_, :_}, :_}

      :ets.match_object(@table, pattern)
      |> Enum.sort_by(fn {{_t, _u, ts, _et}, _payload} -> ts end, :desc)
      |> Enum.take(limit)
      |> Enum.map(fn {{_t, _u, ts, et}, payload} ->
        Map.put(payload, "recorded_at", ts)
        |> Map.put("event_type", et)
      end)
    else
      []
    end
  end

  @doc """
  Summary of recent signals for a user.
  """
  @spec summary(String.t(), String.t() | nil) :: map()
  def summary(tenant_id, user_id) do
    events = recall(tenant_id, user_id, @max_entries_per_user)

    %{
      "event_count" => length(events),
      "last_workout" => last_event_of_type(events, "fitness.workout.logged"),
      "last_task_completed" => last_event_of_type(events, "gtd.task.completed"),
      "last_chore_completed" => last_event_of_type(events, "chore.task.completed"),
      "last_quest_completed" => last_event_of_type(events, "rpg.quest.completed"),
      "workout_count_7d" => count_recent_of_type(events, "fitness.workout.logged", 7),
      "task_count_7d" => count_recent_of_type(events, "gtd.task.completed", 7),
      "days_since_workout" => days_since_event(events, "fitness.workout.logged"),
      "days_since_task" => days_since_event(events, "gtd.task.completed"),
      "recent_events" => Enum.take(events, 5)
    }
  end

  @doc """
  List of event types the broker cares about.
  """
  @spec tracked_event_types() :: [String.t()]
  def tracked_event_types, do: @memory_event_types

  # Callbacks

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :ordered_set, :public, read_concurrency: true])
    Logger.info("[MemoryBroker] Started timeline store")
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:record_event, tenant_id, user_id, event_type, payload}, state) do
    ts = System.system_time(:millisecond)
    key = {tenant_id || default_tenant_id(), user_id || "anonymous", ts, event_type}
    :ets.insert(@table, {key, compact_payload(payload)})
    prune_old_entries(tenant_id, user_id)
    {:noreply, state}
  end

  # Helpers

  defp compact_payload(payload) do
    payload
    |> Map.take([
      "title",
      "name",
      "workout_type",
      "duration_minutes",
      "xp_awarded",
      "character_level",
      "goal",
      "urgency",
      "rarity",
      "task_id",
      "quest_id"
    ])
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new()
  end

  defp prune_old_entries(tenant_id, user_id) do
    pattern = {{tenant_id || default_tenant_id(), user_id || "anonymous", :_, :_}, :_}

    all =
      :ets.match_object(@table, pattern)
      |> Enum.sort_by(fn {{_t, _u, ts, _et}, _payload} -> ts end, :desc)

    if length(all) > @max_entries_per_user do
      all
      |> Enum.drop(@max_entries_per_user)
      |> Enum.each(fn {key, _payload} -> :ets.delete(@table, key) end)
    end
  end

  defp last_event_of_type(events, type) do
    case Enum.find(events, &(&1["event_type"] == type)) do
      nil ->
        nil

      event ->
        Map.take(event, ["recorded_at", "title", "name", "workout_type", "duration_minutes"])
    end
  end

  defp count_recent_of_type(events, type, days) do
    cutoff = System.system_time(:millisecond) - days * 86_400 * 1000

    Enum.count(events, fn e ->
      e["event_type"] == type and Map.get(e, "recorded_at", 0) > cutoff
    end)
  end

  defp days_since_event(events, type) do
    case Enum.find(events, &(&1["event_type"] == type)) do
      nil ->
        nil

      event ->
        ms_ago = System.system_time(:millisecond) - Map.get(event, "recorded_at", 0)
        div(ms_ago, 86_400 * 1000)
    end
  end

  defp default_tenant_id do
    System.get_env("BOT_ARMY_TENANT_ID", "00000000-0000-0000-0000-000000000001")
  end
end
