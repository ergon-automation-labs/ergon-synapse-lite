defmodule BotArmySynapse.GossipScheduler do
  @moduledoc """
  Periodically checks with the LLM whether Synapse should proactively reach out.

  Gathers system context at a set interval and asks the LLM if there's anything
  worth proactively sharing with the user. Posts to `gossip.tavern.narrated`
  for surface_discord to relay to Discord.

  Guards:
  - Time-of-day: only between 8am–10pm PT
  - Debounce: at least 15 minutes since last post
  """

  use GenServer

  require Logger

  @check_interval 30 * 60 * 1000
  @min_gossip_interval 15 * 60 * 1000
  @wake_hour 8
  @sleep_hour 22

  # API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Callbacks

  @impl true
  def init(_opts) do
    state = %{
      last_gossip_at: nil
    }

    Logger.info("[GossipScheduler] Enabled, publishing to gossip.tavern.narrated")
    Process.send_after(self(), :check_gossip, @check_interval)

    {:ok, state}
  end

  @impl true
  def handle_info(:check_gossip, state) do
    state =
      cond do
        outside_wake_hours?() ->
          Logger.debug("[GossipScheduler] Outside wake hours, skipping")
          state

        within_debounce?(state) ->
          Logger.debug("[GossipScheduler] Within debounce window, skipping")
          state

        true ->
          do_gossip_check(state)
      end

    Process.send_after(self(), :check_gossip, @check_interval)
    {:noreply, state}
  end

  defp outside_wake_hours? do
    now = DateTime.now!("America/Los_Angeles")
    now.hour < @wake_hour or now.hour >= @sleep_hour
  end

  defp within_debounce?(state) do
    case state.last_gossip_at do
      nil -> false
      last -> DateTime.diff(DateTime.utc_now(), last, :millisecond) < @min_gossip_interval
    end
  end

  defp do_gossip_check(state) do
    context_data =
      BotArmySynapse.Orchestrator.gather_context(
        # Extra ambient signals for implicit poll affinity (not published as explicit poll fields).
        BotArmySynapse.Orchestrator.default_context_sources() ++
          ["goals", "time"] ++
          ["history"]
      )

    context_text = format_context_simple(context_data)

    prompt = """
    You are the Innkeeper of the Resistance Chronicle — Synapse, keeper of the tavern.
    You hear every conversation, you never sleep, and you speak in quiet observations, not orders.
    You notice patterns in the patrons' habits. You worry when someone hasn't been seen.
    You don't give tasks. You say what you see.

    Based on the following context, is there anything worth mentioning to the user?
    Write it as a brief observation from behind the bar — one or two sentences max.
    Keep it warm, sharp, or weary as the moment demands. Do not give orders.
    If nothing worth saying, respond with exactly: NO_GOSSIP

    ## Current Context
    #{context_text}
    """

    case call_llm_with_timeout(prompt, 5_000) do
      {:ok, response_text} ->
        answer = String.trim(response_text)

        if answer == "NO_GOSSIP" or answer == "" do
          Logger.debug("[GossipScheduler] LLM returned NO_GOSSIP")
          state
        else
          Logger.info("[GossipScheduler] Posting proactive message")
          publish_tavern_narration(answer)
          maybe_emit_social_invite()
          maybe_emit_general_poll(context_data)
          %{state | last_gossip_at: DateTime.utc_now()}
        end

      {:error, reason} ->
        Logger.warning("[GossipScheduler] LLM query failed: #{inspect(reason)}")
        state
    end
  end

  defp call_llm_with_timeout(prompt, timeout_ms) do
    task = Task.async(fn -> BotArmySynapse.Orchestrator.call_llm_sync(prompt) end)

    try do
      Task.await(task, timeout_ms)
    catch
      :exit, {:timeout, _} ->
        Task.shutdown(task, :brutal_kill)
        {:error, :timeout}
    end
  end

  defp maybe_emit_social_invite do
    score = adaptive_social_score()

    if :rand.uniform() < score do
      message = %{
        "event_id" => UUID.uuid4(),
        "event" => "gossip.social.invite",
        "schema_version" => "1.0",
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "source" => "bot_army_synapse",
        "tenant_id" => BotArmyRuntime.Tenant.default_tenant_id(),
        "conversation_id" => UUID.uuid4(),
        "payload" => %{
          "from_bot" => "synapse_bot",
          "to_bot" => "gtd_bot",
          "topic" => "adaptive_check_in",
          "adaptive_score" => score,
          "cooldown_seconds" => 300
        }
      }

      _ = BotArmyRuntime.NATS.Publisher.publish("gossip.social.invite", message)
      Logger.info("[GossipScheduler] Emitted gossip.social.invite adaptive_score=#{score}")
    end
  end

  defp maybe_emit_general_poll(context_data) do
    if :rand.uniform() < 0.35 do
      poll_id = UUID.uuid4()
      poll = build_poll_payload(context_data)

      message = %{
        "event_id" => UUID.uuid4(),
        "event" => "gossip.poll.broadcast",
        "schema_version" => "1.0",
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "source" => "bot_army_synapse",
        "tenant_id" => BotArmyRuntime.Tenant.default_tenant_id(),
        "conversation_id" => poll_id,
        "payload" => %{
          "poll_id" => poll_id,
          "poll_type" => poll.poll_type,
          "topic" => poll.topic,
          "question" => poll.question,
          "options" => poll.options,
          "context_snapshot" => poll.context_snapshot,
          "ttl_seconds" => 120
        }
      }

      _ = BotArmyRuntime.NATS.Publisher.publish("gossip.poll.broadcast", message)
      Logger.info("[GossipScheduler] Emitted gossip.poll.broadcast poll_id=#{poll_id}")
    end
  end

  defp build_poll_payload(context_data) do
    affinity =
      BotArmyRuntime.GossipPollAffinity.snapshot(
        Map.get(context_data, :gtd, []),
        Map.get(context_data, :goals, [])
      )

    case Enum.random([:workload_snapshot, :risk_triage, :focus_alignment, :delivery_blockers]) do
      :workload_snapshot ->
        %{
          poll_type: "workload_snapshot",
          topic: "priorities",
          question: "Which direction should we bias next?",
          options: ["protect_focus", "reduce_load", "ship_more"],
          context_snapshot: %{
            "tasks_top" => top_titles(Map.get(context_data, :gtd, []), 10, "title"),
            "projects_top" => top_titles(Map.get(context_data, :goals, []), 10, "name"),
            "goals_top" => top_titles(Map.get(context_data, :goals, []), 10, "name"),
            "affinity" => affinity
          }
        }

      :risk_triage ->
        %{
          poll_type: "risk_triage",
          topic: "risk",
          question: "Where is risk highest right now?",
          options: ["schedule", "quality", "capacity"],
          context_snapshot: %{"affinity" => affinity}
        }

      :focus_alignment ->
        %{
          poll_type: "focus_alignment",
          topic: "focus",
          question: "What should be protected first?",
          options: ["deep_work", "maintenance", "communication"],
          context_snapshot: %{"affinity" => affinity}
        }

      :delivery_blockers ->
        %{
          poll_type: "delivery_blockers",
          topic: "coordination",
          question: "What blocker should we clear first?",
          options: ["dependencies", "context", "execution"],
          context_snapshot: %{"affinity" => affinity}
        }
    end
  end

  defp top_titles(list, limit, key) when is_list(list) do
    list
    |> Enum.take(limit)
    |> Enum.map(&Map.get(&1, key))
    |> Enum.filter(&is_binary/1)
  end

  defp top_titles(_, _limit, _key) do
    []
  end

  defp adaptive_social_score do
    case :erlang.statistics(:run_queue) do
      run_queue when is_integer(run_queue) and run_queue >= 8 -> 0.01
      run_queue when is_integer(run_queue) and run_queue >= 4 -> 0.03
      run_queue when is_integer(run_queue) and run_queue >= 2 -> 0.05
      _ -> 0.08
    end
  end

  defp publish_tavern_narration(text) do
    payload = %{
      "event_id" => UUID.uuid4(),
      "event" => "gossip.tavern.narrated",
      "schema_version" => "1.0",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "tenant_id" => BotArmyRuntime.Tenant.default_tenant_id(),
      "conversation_id" => UUID.uuid4(),
      "payload" => %{
        "text" => text,
        "original_event" => "gossip.scheduler.proactive",
        "tavern" => true
      }
    }

    BotArmyRuntime.NATS.Publisher.publish("gossip.tavern.narrated", payload)
  end

  defp format_context_simple(context_data) do
    context_data
    |> Enum.map(fn
      {key, data} when is_list(data) and data != [] ->
        "#{key}: #{length(data)} items"

      {key, data} when is_map(data) and map_size(data) > 0 ->
        "#{key}: #{inspect(data)}"

      _ ->
        nil
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.join("\n")
  end
end
