defmodule BotArmySynapse.DecisionEngine do
  @moduledoc """
  Rules-based decision engine for triggering Claude sessions.

  Evaluates fleet events against configurable rules to determine
  when something needs Claude's deeper attention. When a rule fires,
  publishes a `bot_army.claude.trigger.brain` envelope via TriggerPublisher.

  ## Rules (initial set)

  - **High-priority task accumulation**: 3+ high-priority GTD tasks unprocessed
  - **Calendar deadline proximity**: event starting within 1 hour
  - **Context mode shift**: context mode changed to "deep_work" or "focus"
  - **Stale inbox**: GTD inbox items older than 24h with no processing
  - **Goal at risk**: Active goal has 0 decisions in 7 days AND 3+ stagnant tasks
  - **Cross-goal dependency**: 2+ goals in same area both have urgent tasks

  Rules are evaluated when events arrive via the NATS consumer.
  Each rule is a function that takes context data and returns
  `{:trigger, prompt, opts}` or `:no_action`.

  ## Configuration

  Rules can be enabled/disabled via env vars or Salt pillar:
  - `SYNAPSE_DECISION_RULES` — comma-separated list of active rules (default: all)
  - `SYNAPSE_CLAUDE_TRIGGER_ENABLED` — master switch (default: `true`)
  """

  use GenServer
  require Logger

  alias BotArmySynapse.TriggerPublisher

  @default_rules [
    :high_priority_accumulation,
    :deadline_proximity,
    :context_mode_shift,
    :stale_inbox,
    :goal_at_risk,
    :cross_goal_dependency,
    :job_listings_spike,
    :sre_alert,
    :stale_game
  ]

  # API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def evaluate(event_type, context_data) do
    GenServer.cast(__MODULE__, {:evaluate, event_type, context_data})
  end

  # Callbacks

  @impl true
  def init(opts) do
    enabled =
      case System.get_env("SYNAPSE_CLAUDE_TRIGGER_ENABLED") do
        "false" -> false
        _ -> true
      end

    active_rules =
      case System.get_env("SYNAPSE_DECISION_RULES") do
        nil ->
          @default_rules

        rules_str ->
          rules_str
          |> String.split(",")
          |> Enum.map(&String.trim/1)
          |> Enum.map(&String.to_atom/1)
      end

    state = %{
      enabled: enabled,
      active_rules: active_rules,
      opts: opts,
      # Track recent triggers to avoid duplicate firing
      recent_triggers: %{},
      cooldown_ms: 15 * 60 * 1_000
    }

    # Schedule hourly periodic checks for goal-aware rules
    Process.send_after(self(), :periodic_check, 60 * 60 * 1_000)

    {:ok, state}
  end

  @impl true
  def handle_cast({:evaluate, event_type, context_data}, state) do
    new_state =
      if state.enabled do
        evaluate_rules(event_type, context_data, state)
      else
        state
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:clear_cooldown, rule_key}, state) do
    recent_triggers = Map.delete(state.recent_triggers, rule_key)
    {:noreply, %{state | recent_triggers: recent_triggers}}
  end

  @impl true
  def handle_info(:periodic_check, state) do
    Logger.debug("[DecisionEngine] Running periodic goal health checks")
    new_state = evaluate_rules(:periodic_check, %{}, state)

    Process.send_after(self(), :periodic_check, 60 * 60 * 1_000)
    {:noreply, new_state}
  end

  # Rule evaluation

  defp evaluate_rules(event_type, context_data, state) do
    Enum.reduce(state.active_rules, state, fn rule, acc_state ->
      if within_cooldown?(rule, event_type, acc_state) do
        Logger.debug("[DecisionEngine] Rule #{rule} on cooldown, skipping")
        acc_state
      else
        case apply_rule(rule, event_type, context_data) do
          {:trigger, prompt, opts} ->
            Logger.info("[DecisionEngine] Rule #{rule} fired: #{String.slice(prompt, 0, 80)}")
            fire_trigger(rule, event_type, prompt, opts, acc_state)

          :no_action ->
            acc_state
        end
      end
    end)
  end

  defp within_cooldown?(rule, event_type, state) do
    rule_key = {rule, event_type}
    Map.has_key?(state.recent_triggers, rule_key)
  end

  defp fire_trigger(rule, event_type, prompt, opts, state) do
    rule_key = {rule, event_type}
    cooldown = Keyword.get(opts, :cooldown_ms, state.cooldown_ms)

    Process.send_after(self(), {:clear_cooldown, rule_key}, cooldown)
    recent_triggers = Map.put(state.recent_triggers, rule_key, DateTime.utc_now())

    trigger_opts = %{
      launcher: Keyword.get(opts, :launcher, "ollama"),
      model: Keyword.get(opts, :model, "glm-5.1:cloud"),
      max_turns: Keyword.get(opts, :max_turns, 10),
      max_budget_usd: Keyword.get(opts, :max_budget_usd, 1.0),
      context: Keyword.get(opts, :context, %{})
    }

    TriggerPublisher.publish(prompt, trigger_opts)
    %{state | recent_triggers: recent_triggers}
  end

  # --- Rules ---

  defp apply_rule(:high_priority_accumulation, event_type, context_data)
       when event_type in [:gtd_task_updated, :gtd_task_created] do
    tasks = Map.get(context_data, :gtd_tasks) || []

    high_priority =
      Enum.filter(tasks, fn task ->
        task["priority"] in ["high", "urgent", "1"] and
          task["status"] in ["inbox", "pending", "next_action"]
      end)

    if length(high_priority) >= 3 do
      task_list = Enum.map_join(high_priority, "\n", &"  - #{&1["title"]} [#{&1["priority"]}]")

      prompt = """
      There are #{length(high_priority)} high-priority tasks that need attention:

      #{task_list}

      Please analyze these tasks and suggest:
      1. Which should be tackled first
      2. Any dependencies between them
      3. Whether any should be broken down into smaller steps
      """

      {:trigger, prompt,
       launcher: "ollama",
       model: "glm-5.1:cloud",
       context: %{high_priority_count: length(high_priority)}}
    else
      :no_action
    end
  end

  defp apply_rule(:deadline_proximity, :calendar_event, context_data) do
    events = Map.get(context_data, :calendar_events, [])

    urgent =
      Enum.filter(events, fn event ->
        case Map.get(event, "starts_in_minutes") do
          mins when is_number(mins) and mins >= 0 and mins <= 60 -> true
          _ -> false
        end
      end)

    if urgent != [] do
      event_list =
        Enum.map_join(
          urgent,
          "\n",
          &"  - #{&1["summary"]} (starts in #{&1["starts_in_minutes"]}min)"
        )

      prompt = """
      Upcoming calendar events requiring preparation:

      #{event_list}

      Please review and suggest preparation steps for each event.
      """

      {:trigger, prompt,
       launcher: "ollama", model: "glm-5.1:cloud", context: %{urgent_events: length(urgent)}}
    else
      :no_action
    end
  end

  defp apply_rule(:context_mode_shift, :context_state_changed, context_data) do
    mode = get_in(context_data, [:context_state, "mode"])

    if mode in ["deep_work", "focus"] do
      # When entering deep work/focus mode, trigger Claude to prepare context
      focus = get_in(context_data, [:context_state, "focus"]) || "unspecified focus area"

      prompt = """
      User has entered #{mode} mode with focus on: #{focus}

      Please gather relevant context and prepare a summary of what they should work on.
      Check for any high-priority items related to the focus area.
      """

      {:trigger, prompt,
       launcher: "ollama", model: "glm-5.1:cloud", context: %{mode: mode, focus: focus}}
    else
      :no_action
    end
  end

  defp apply_rule(:stale_inbox, event_type, context_data)
       when event_type in [:gtd_task_updated, :gtd_task_created, :periodic_check] do
    tasks = Map.get(context_data, :gtd_tasks) || []

    stale_inbox =
      Enum.filter(tasks, fn task ->
        task["status"] == "inbox" and
          task_older_than_24h?(task)
      end)

    if length(stale_inbox) >= 3 do
      task_list =
        Enum.map_join(
          stale_inbox,
          "\n",
          &"  - #{&1["title"]} (in inbox since #{&1["created_at"]})"
        )

      prompt = """
      There are #{length(stale_inbox)} GTD inbox items older than 24 hours that need processing:

      #{task_list}

      Please suggest how to process each inbox item:
      - Should it be a next action, project, waiting-for, or someday/maybe?
      - What context should it be assigned?
      - Are there any that can be combined or deleted?
      """

      {:trigger, prompt,
       launcher: "ollama",
       model: "glm-5.1:cloud",
       context: %{stale_inbox_count: length(stale_inbox)}}
    else
      :no_action
    end
  end

  defp apply_rule(:goal_at_risk, event_type, _context_data)
       when event_type in [:gtd_task_updated, :periodic_check] do
    try do
      goals = BotArmySynapse.GoalStore.list_goals(:active)

      case goals do
        nil ->
          :no_action

        [] ->
          :no_action

        goals_list when is_list(goals_list) ->
          at_risk = Enum.filter(goals_list, &goal_at_risk?/1)

          if Enum.empty?(at_risk) do
            :no_action
          else
            goal_list = Enum.map_join(at_risk, "\n", &"  - #{&1["name"]} (#{&1["status"]})")

            prompt = """
            The following goals are at risk — no decisions recorded in 7+ days:

            #{goal_list}

            Please review these goals and suggest:
            1. Whether to reactivate, archive, or refocus them
            2. What might be blocking progress
            3. Any decomposition that would help restart momentum
            """

            {:trigger, prompt,
             launcher: "ollama",
             model: "glm-5.1:cloud",
             cooldown_ms: 24 * 60 * 60 * 1_000,
             context: %{
               at_risk_goals: length(at_risk),
               goal_names: Enum.map(at_risk, & &1["name"])
             }}
          end

        _ ->
          Logger.warning(
            "[DecisionEngine] goal_at_risk: unexpected goals type: #{inspect(goals, limit: 100)}"
          )

          :no_action
      end
    rescue
      e ->
        Logger.error("[DecisionEngine] goal_at_risk error: #{inspect(e)}")
        :no_action
    end
  end

  defp apply_rule(:cross_goal_dependency, event_type, _context_data)
       when event_type in [:gtd_task_updated, :periodic_check] do
    goals = BotArmySynapse.GoalStore.list_goals(:active)

    case goals do
      nil ->
        :no_action

      [] ->
        :no_action

      _ ->
        by_area = Enum.group_by(goals, &Map.get(&1, "area", "general"))

        conflicts =
          Enum.filter(by_area, fn {_area, goals_in_area} -> length(goals_in_area) >= 2 end)

        if Enum.empty?(conflicts) do
          :no_action
        else
          conflict_list =
            Enum.map_join(conflicts, "\n", fn {area, gs} ->
              goal_names = Enum.map_join(gs, ", ", &"#{&1["name"]}")
              "  - [#{area}] #{goal_names}"
            end)

          prompt = """
          Multiple goals in the same area have active urgent tasks:

          #{conflict_list}

          Please analyze these goals and suggest:
          1. Which tasks can be parallelized
          2. Which should be sequenced first
          3. Any resource conflicts that need resolving
          """

          {
            :trigger,
            prompt,
            # conflicts is a filtered list of {area, goals[]} tuples
            launcher: "ollama",
            model: "glm-5.1:cloud",
            context: %{conflicted_areas: length(conflicts)}
          }
        end
    end
  end

  defp apply_rule(:job_listings_spike, :listing_added, _context_data) do
    recent_listings = BotArmySynapse.EventHistory.get_recent_by_type(:listing_added)

    one_hour_ago = DateTime.add(DateTime.utc_now(), -3600, :second)

    recent_count =
      Enum.count(recent_listings, fn event ->
        DateTime.compare(event.timestamp, one_hour_ago) == :gt
      end)

    if recent_count >= 5 do
      listing_summaries =
        recent_listings
        |> Enum.take(10)
        |> Enum.map_join("\n", &"  - #{&1.summary}")

      prompt = """
      #{recent_count} new job listings have been added in the last hour:

      #{listing_summaries}

      Please review these listings and suggest:
      1. Which are the best matches based on the user's profile
      2. Any notable companies or roles
      3. Recommended priority for applying
      """

      {:trigger, prompt,
       launcher: "ollama", model: "glm-5.1:cloud", context: %{recent_listings: recent_count}}
    else
      :no_action
    end
  end

  defp apply_rule(:sre_alert, :sre_alert, context_data) do
    alerts = Map.get(context_data, :sre_alerts)

    if is_map(alerts) and map_size(alerts) > 0 do
      prompt = """
      SRE alert detected: #{inspect(alerts)}

      Please help prepare an investigation:
      1. What might be causing this alert?
      2. What logs or metrics should be checked?
      3. What runbook steps apply?
      """

      {:trigger, prompt,
       launcher: "ollama", model: "glm-5.1:cloud", context: %{alert_data: alerts}}
    else
      :no_action
    end
  end

  defp apply_rule(:stale_game, :periodic_check, _context_data) do
    recent_games = BotArmySynapse.EventHistory.get_recent_by_type(:game_generated)

    stale_game =
      Enum.find(recent_games, fn event ->
        age_hours = DateTime.diff(DateTime.utc_now(), event.timestamp, :hour)
        age_hours > 24
      end)

    if stale_game do
      prompt = """
      A terrain/dungeon game question has been unanswered for over 24 hours:

      #{stale_game.summary}

      Please suggest:
      1. Whether to review the game question
      2. Tips for approaching this type of question
      """

      {:trigger, prompt,
       launcher: "ollama", model: "glm-5.1:cloud", context: %{game_age_hours: 24}}
    else
      :no_action
    end
  end

  defp apply_rule(_rule, _event_type, _context_data), do: :no_action

  defp task_older_than_24h?(task) do
    case Map.get(task, "created_at") do
      nil ->
        false

      timestamp ->
        case DateTime.from_iso8601(timestamp) do
          {:ok, dt, _} -> DateTime.diff(DateTime.utc_now(), dt, :hour) >= 24
          _ -> false
        end
    end
  end

  defp goal_at_risk?(goal) do
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
end
