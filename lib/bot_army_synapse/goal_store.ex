defmodule BotArmySynapse.GoalStore do
  @moduledoc """
  In-memory cache of GTD projects (goals) with synapse enrichment.

  GTD is the persistence layer (via gtd.project.list/create/update). This GenServer
  maintains a warm cache refreshed every 5 minutes, enriched with synapse-side metadata:
  decision history, progress summaries, context sources used, and activity timestamps.

  Atom-keyed enrichment fields never collide with GTD's string-keyed project fields.
  """

  use GenServer
  require Logger

  # 5 minutes
  @sync_interval_ms 5 * 60 * 1000
  @nats_timeout 3000

  # API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "List all goals matching the given filter."
  def list_goals(filter \\ :active) do
    GenServer.call(__MODULE__, {:list_goals, filter})
  rescue
    _e -> nil
  end

  @doc "Get a single goal by project ID, or nil."
  def get_goal(project_id) when is_binary(project_id) do
    GenServer.call(__MODULE__, {:get_goal, project_id})
  rescue
    _e -> nil
  end

  @doc "Get a single goal by name, or nil."
  def get_goal_by_name(name) when is_binary(name) do
    GenServer.call(__MODULE__, {:get_goal_by_name, name})
  rescue
    _e -> nil
  end

  @doc "Trigger an immediate sync from GTD."
  def refresh do
    GenServer.cast(__MODULE__, :refresh)
  end

  @doc "Record a decision made in the context of a goal."
  def record_decision(project_id, summary) when is_binary(project_id) and is_binary(summary) do
    GenServer.cast(__MODULE__, {:record_decision, project_id, summary})
  end

  @doc "Update the progress summary for a goal."
  def update_progress(project_id, summary) when is_binary(project_id) and is_binary(summary) do
    GenServer.cast(__MODULE__, {:update_progress, project_id, summary})
  end

  # Callbacks

  @impl true
  def init(_opts) do
    Logger.info("[GoalStore] Starting with initial sync")

    state = %{
      goals: %{},
      last_sync: nil,
      sync_interval_ms: @sync_interval_ms,
      sync_failures: 0,
      last_error: nil
    }

    {:ok, state, {:continue, :initial_sync}}
  end

  @impl true
  def handle_continue(:initial_sync, state) do
    case fetch_projects_from_gtd() do
      {:ok, projects} ->
        enriched = enrich_projects(projects, %{})
        Logger.info("[GoalStore] Initial sync: #{length(projects)} goals loaded")

        new_state = %{
          state
          | goals: enriched,
            last_sync: DateTime.utc_now(),
            sync_failures: 0,
            last_error: nil
        }

        publish_health(new_state)
        BotArmySynapse.ArmyContext.publish_goal_context(enriched)
        {:noreply, new_state, {:continue, :schedule_next_sync}}

      {:error, reason} ->
        Logger.warning(
          "[GoalStore] Initial sync failed: #{inspect(reason)}, will retry at next interval"
        )

        new_state = %{state | sync_failures: state.sync_failures + 1, last_error: inspect(reason)}
        publish_health(new_state)
        {:noreply, new_state, {:continue, :schedule_next_sync}}
    end
  end

  @impl true
  def handle_continue(:schedule_next_sync, state) do
    Process.send_after(self(), :sync, @sync_interval_ms)
    {:noreply, state}
  end

  @impl true
  def handle_info(:sync, state) do
    case fetch_projects_from_gtd() do
      {:ok, projects} ->
        enriched = enrich_projects(projects, state.goals)

        new_state = %{
          state
          | goals: enriched,
            last_sync: DateTime.utc_now(),
            sync_failures: 0,
            last_error: nil
        }

        publish_health(new_state)
        BotArmySynapse.ArmyContext.publish_goal_context(enriched)
        {:noreply, new_state, {:continue, :schedule_next_sync}}

      {:error, reason} ->
        # Sync failed, keep existing goals and retry at next interval
        new_state = %{
          state
          | sync_failures: state.sync_failures + 1,
            last_error: inspect(reason)
        }

        publish_health(new_state)
        {:noreply, new_state, {:continue, :schedule_next_sync}}
    end
  end

  @impl true
  def handle_call({:list_goals, filter}, _from, state) do
    goals =
      state.goals
      |> Map.values()
      |> filter_goals(filter)
      |> Enum.sort_by(& &1["name"])

    {:reply, goals, state}
  end

  @impl true
  def handle_call({:get_goal, project_id}, _from, state) do
    goal = Map.get(state.goals, project_id)
    {:reply, goal, state}
  end

  @impl true
  def handle_call({:get_goal_by_name, name}, _from, state) do
    goal =
      state.goals
      |> Map.values()
      |> Enum.find(&(&1["name"] == name))

    {:reply, goal, state}
  end

  @impl true
  def handle_cast(:refresh, state) do
    case fetch_projects_from_gtd() do
      {:ok, projects} ->
        enriched = enrich_projects(projects, state.goals)
        {:noreply, %{state | goals: enriched, last_sync: DateTime.utc_now()}}

      {:error, _reason} ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:record_decision, project_id, _summary}, state) do
    goals =
      Map.update(state.goals, project_id, nil, fn goal ->
        if goal do
          goal
          |> Map.update(:decision_count, 1, &(&1 + 1))
          |> Map.put(:last_decision_at, DateTime.utc_now())
          |> Map.put(:last_active_at, DateTime.utc_now())
        else
          goal
        end
      end)

    {:noreply, %{state | goals: goals}}
  end

  @impl true
  def handle_cast({:update_progress, project_id, summary}, state) do
    goals =
      Map.update(state.goals, project_id, nil, fn goal ->
        if goal do
          goal
          |> Map.put(:progress_summary, summary)
          |> Map.put(:last_active_at, DateTime.utc_now())
        else
          goal
        end
      end)

    {:noreply, %{state | goals: goals}}
  end

  # Private

  defp fetch_projects_from_gtd do
    case BotArmySynapse.PulseListener.get_bot_health("gtd") do
      pulse when is_map(pulse) ->
        projects = Map.get(pulse, "projects", [])
        Logger.debug("[GoalStore] Fetched #{length(projects)} projects from GTD pulse cache")
        {:ok, projects}

      nil ->
        Logger.debug("[GoalStore] GTD pulse not available, falling back to NATS request")
        fetch_projects_from_gtd_nats()
    end
  rescue
    e ->
      Logger.warning("[GoalStore] Exception fetching projects from pulse: #{inspect(e)}")
      {:error, e}
  end

  defp fetch_projects_from_gtd_nats do
    with {:ok, conn} <-
           GenServer.call(BotArmyLibraryRuntime.NATS.Connection, :get_connection, @nats_timeout),
         {:ok, response} <-
           Gnat.request(
             conn,
             "gtd.project.list",
             Jason.encode!(BotArmySynapse.build_envelope("gtd.project.list")),
             receive_timeout: @nats_timeout
           ),
         {:ok, decoded} <- Jason.decode(extract_nats_body(response)) do
      projects = get_in(decoded, ["data", "projects"]) || []
      {:ok, projects}
    else
      {:error, reason} ->
        Logger.warning(
          "[GoalStore] Failed to fetch projects from GTD via NATS: #{inspect(reason)}"
        )

        {:error, reason}

      other ->
        Logger.warning("[GoalStore] Unexpected error fetching projects: #{inspect(other)}")
        {:error, other}
    end
  rescue
    e ->
      Logger.warning("[GoalStore] Exception fetching projects via NATS: #{inspect(e)}")
      {:error, e}
  end

  defp extract_nats_body(%{body: body}), do: body
  defp extract_nats_body(body) when is_binary(body), do: body

  defp enrich_projects(gtd_projects, existing_goals) do
    Enum.reduce(gtd_projects, %{}, fn gtd_project, acc ->
      project_id = gtd_project["id"]

      # Merge GTD fields (strings) with existing synapse enrichment (atoms)
      enriched =
        case Map.get(existing_goals, project_id) do
          nil ->
            # New project: initialize with defaults
            Map.merge(gtd_project, %{
              decision_count: 0,
              last_decision_at: nil,
              progress_summary: nil,
              context_sources_used: [],
              last_active_at: nil,
              synced_at: DateTime.utc_now()
            })

          existing ->
            # Existing project: preserve synapse enrichment, update GTD fields
            Map.merge(existing, Map.merge(gtd_project, %{synced_at: DateTime.utc_now()}))
        end

      Map.put(acc, project_id, enriched)
    end)
  end

  defp publish_health(_state) do
    # goal_store is an internal Synapse service; its health is included in Synapse's
    # overall health report via the runtime Health.Responder. Disabled separate
    # publishing to avoid "goal_store (silent 60s)" false warnings in all bots.
    :ok
  end

  defp filter_goals(goals, :all), do: goals

  defp filter_goals(goals, :active) do
    Enum.filter(goals, &(&1["status"] == "active"))
  end

  defp filter_goals(goals, :completed) do
    Enum.filter(goals, &(&1["status"] == "completed"))
  end

  defp filter_goals(goals, _), do: goals
end
