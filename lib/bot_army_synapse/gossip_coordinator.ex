defmodule BotArmySynapse.GossipCoordinator do
  @moduledoc """
  Coordinates topic-agnostic gossip flows for Synapse.

  Synapse acts as the lightweight resolver:
  - receives intent proposals and peer answers
  - publishes clarification requests when context is missing
  - emits resolved decisions after receiving peer input
  """

  use GenServer
  require Logger

  @table :synapse_gossip_state

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def record_intent_proposed(message), do: GenServer.cast(__MODULE__, {:intent_proposed, message})
  def record_intent_answer(message), do: GenServer.cast(__MODULE__, {:intent_answer, message})
  def record_social_reply(message), do: GenServer.cast(__MODULE__, {:social_reply, message})
  def record_poll_broadcast(message), do: GenServer.cast(__MODULE__, {:poll_broadcast, message})
  def record_poll_vote(message), do: GenServer.cast(__MODULE__, {:poll_vote, message})

  def record_gtd_poll_broadcast(message),
    do: GenServer.cast(__MODULE__, {:gtd_poll_broadcast, message})

  def maybe_vote_on_heartbeat, do: GenServer.cast(__MODULE__, :heartbeat_vote)

  @heartbeat_interval_ms 300_000

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])

    if heartbeat_gossip_enabled?() do
      schedule_heartbeat()
    else
      Logger.info("[GossipCoordinator] Heartbeat gossip disabled (SYNAPSE_HEARTBEAT_GOSSIP != 1)")
    end

    {:ok,
     %{
       active_poll: nil,
       voted_poll_ids: MapSet.new(),
       active_gtd_poll: nil,
       voted_gtd_poll_ids: MapSet.new()
     }}
  end

  @impl true
  def handle_cast({:intent_proposed, message}, state) do
    payload = Map.get(message, "payload", %{})
    intent_key = Map.get(payload, "intent_key", "")

    if intent_key != "" do
      :ets.insert(@table, {intent_key, %{message: message, answers: [], resolved?: false}})
      Logger.info("[GossipCoordinator] tracked intent proposal intent_key=#{intent_key}")
      maybe_publish_tavern(message)
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:intent_answer, message}, state) do
    payload = Map.get(message, "payload", %{})
    intent_key = Map.get(payload, "intent_key", "")

    case :ets.lookup(@table, intent_key) do
      [{^intent_key, entry}] ->
        updated = %{entry | answers: [message | entry.answers]}
        :ets.insert(@table, {intent_key, updated})

        maybe_resolve(intent_key, updated)
        {:noreply, state}

      [] ->
        Logger.debug("[GossipCoordinator] answer for unknown intent_key=#{intent_key}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:social_reply, message}, state) do
    payload = Map.get(message, "payload", %{})
    Logger.info("[GossipCoordinator] social reply accepted=#{inspect(payload["accepted"])}")
    maybe_publish_tavern(message)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:poll_broadcast, message}, state) do
    payload = Map.get(message, "payload", %{})
    poll_id = Map.get(payload, "poll_id")
    topic = Map.get(payload, "topic", "unknown")
    options = Map.get(payload, "options", [])
    context_snapshot = Map.get(payload, "context_snapshot", %{})
    ttl_seconds = Map.get(payload, "ttl_seconds", 60)

    if is_binary(poll_id) and poll_id != "" do
      expires_at = System.system_time(:second) + ttl_seconds
      Logger.info("[GossipCoordinator] poll broadcast poll_id=#{poll_id} topic=#{topic}")
      Process.send_after(self(), {:finalize_poll, poll_id}, ttl_seconds * 1000)
      maybe_publish_tavern(message)

      {:noreply,
       %{
         state
         | active_poll: %{
             poll_id: poll_id,
             topic: topic,
             options: options,
             context_snapshot: context_snapshot,
             expires_at: expires_at,
             votes: %{}
           }
       }}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:poll_vote, message}, state) do
    payload = Map.get(message, "payload", %{})
    poll_id = Map.get(payload, "poll_id", "unknown")
    voter = Map.get(payload, "voter", "unknown")
    vote = Map.get(payload, "vote", "unknown")
    Logger.info("[GossipCoordinator] poll vote poll_id=#{poll_id} voter=#{voter} vote=#{vote}")

    updated_state =
      case state.active_poll do
        %{poll_id: ^poll_id} = poll ->
          normalized_vote = normalize_vote(vote, poll)

          %{
            state
            | active_poll: %{poll | votes: Map.put(poll.votes || %{}, voter, normalized_vote)}
          }

        _ ->
          state
      end

    {:noreply, updated_state}
  end

  @impl true
  def handle_cast(:heartbeat_vote, state) do
    now = System.system_time(:second)

    state =
      cond do
        is_nil(state.active_poll) ->
          state

        now > state.active_poll.expires_at ->
          %{state | active_poll: nil}

        MapSet.member?(state.voted_poll_ids, state.active_poll.poll_id) ->
          state

        true ->
          publish_poll_vote(state.active_poll)
          %{state | voted_poll_ids: MapSet.put(state.voted_poll_ids, state.active_poll.poll_id)}
      end

    state = maybe_vote_on_gtd_poll(state)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:gtd_poll_broadcast, message}, state) do
    payload = Map.get(message, "payload", message)
    poll_id = Map.get(payload, "poll_id")
    choices = Map.get(payload, "choices", %{})
    budget = Map.get(payload, "vote_budget_per_bot", 3)
    tenant_id = Map.get(payload, "tenant_id", BotArmyRuntime.Tenant.default_tenant_id())

    if is_binary(poll_id) and poll_id != "" do
      Logger.info("[GossipCoordinator] GTD poll broadcast poll_id=#{poll_id}")
      maybe_publish_tavern(message)

      {:noreply,
       %{
         state
         | active_gtd_poll: %{
             poll_id: poll_id,
             choices: choices,
             budget: budget,
             tenant_id: tenant_id
           }
       }}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:finalize_poll, poll_id}, state) do
    case state.active_poll do
      %{poll_id: ^poll_id} = poll ->
        {winner, counts} = tally_votes(poll.votes || %{})
        publish_poll_resolved(poll, winner, counts)
        maybe_publish_discord_poll_summary(poll, winner, counts)
        {:noreply, %{state | active_poll: nil}}

      _ ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:clear_gtd_poll, state) do
    {:noreply, %{state | active_gtd_poll: nil}}
  end

  @impl true
  def handle_info(:heartbeat_gossip, state) do
    if not heartbeat_gossip_enabled?() do
      {:noreply, state}
    else
      spawn(fn ->
        case BotArmyRuntime.Registry.list_bots() do
          {:ok, bots} ->
            active = Enum.filter(bots, fn b -> b["status"] == "active" end)
            stale = Enum.filter(bots, fn b -> b["status"] == "stale" end)
            count = length(active)
            stale_count = length(stale)

            intent_key =
              :crypto.hash(:sha256, "heartbeat:#{DateTime.utc_now() |> DateTime.to_date()}")
              |> Base.encode16(case: :lower)
              |> String.slice(0, 16)

            topic =
              if stale_count > 0 do
                "system.healing.coordinate"
              else
                "system.status.check"
              end

            proposed = %{
              "event_id" => UUID.uuid4(),
              "event" => "gossip.intent.proposed",
              "schema_version" => "1.0",
              "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
              "source" => "bot_army_synapse",
              "tenant_id" => "00000000-0000-0000-0000-000000000001",
              "conversation_id" => UUID.uuid4(),
              "payload" => %{
                "intent_key" => intent_key,
                "topic" => topic,
                "summary" => "Fleet heartbeat: #{count} active, #{stale_count} stale bots.",
                "target_bots" =>
                  Enum.map(active, fn b -> b["name"] || "unknown" end) |> Enum.take(3),
                "hop_count" => 0,
                "ttl_seconds" => 300,
                "metadata" => %{
                  "mode" => "utility",
                  "priority" => "background",
                  "trigger" => "heartbeat",
                  "active_count" => count,
                  "stale_count" => stale_count
                }
              }
            }

            BotArmyRuntime.NATS.Publisher.publish("gossip.intent.proposed", proposed)
            maybe_publish_tavern(proposed)

            target_bots = Map.get(proposed["payload"], "target_bots", [])

            Enum.with_index(target_bots)
            |> Enum.take(2)
            |> Enum.each(fn {bot_name, idx} ->
              :timer.sleep(2_000)

              stance =
                if stale_count > 0 and idx == 0 do
                  "need_more_context"
                else
                  "already_have_equivalent"
                end

              answer = %{
                "event_id" => UUID.uuid4(),
                "event" => "gossip.intent.answer",
                "schema_version" => "1.0",
                "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
                "source" => bot_name,
                "tenant_id" => "00000000-0000-0000-0000-000000000001",
                "conversation_id" => Map.get(proposed, "conversation_id", UUID.uuid4()),
                "payload" => %{
                  "intent_key" => intent_key,
                  "responder" => bot_name,
                  "stance" => stance,
                  "reason" => "Heartbeat auto-answer from fleet registry."
                }
              }

              BotArmyRuntime.NATS.Publisher.publish("gossip.intent.answer", answer)
            end)

          {:error, _} ->
            :ok
        end
      end)

      schedule_heartbeat()
      {:noreply, state}
    end
  end

  defp schedule_heartbeat do
    Process.send_after(self(), :heartbeat_gossip, @heartbeat_interval_ms)
  end

  defp heartbeat_gossip_enabled? do
    case System.get_env("SYNAPSE_HEARTBEAT_GOSSIP") do
      "1" -> true
      "true" -> true
      "TRUE" -> true
      _ -> false
    end
  end

  defp maybe_resolve(_intent_key, %{resolved?: true}), do: :ok

  defp maybe_resolve(intent_key, %{answers: answers, message: proposed} = entry) do
    if length(answers) >= 1 do
      decision = decide(answers)
      publish_resolved(proposed, intent_key, decision)
      execute_and_narrate(proposed, intent_key, decision)
      :ets.insert(@table, {intent_key, %{entry | resolved?: true}})
    end
  end

  defp decide(answers) do
    stances =
      answers
      |> Enum.map(&get_in(&1, ["payload", "stance"]))
      |> Enum.filter(&is_binary/1)

    cond do
      "already_have_equivalent" in stances -> "update_existing"
      "need_more_context" in stances -> "defer"
      true -> "create_new"
    end
  end

  defp publish_resolved(proposed, intent_key, decision) do
    proposed_payload = Map.get(proposed, "payload", %{})

    resolved = %{
      "event_id" => UUID.uuid4(),
      "event" => "gossip.intent.resolved",
      "schema_version" => "1.0",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "tenant_id" => Map.get(proposed, "tenant_id", "00000000-0000-0000-0000-000000000001"),
      "conversation_id" => Map.get(proposed, "conversation_id", UUID.uuid4()),
      "payload" => %{
        "intent_key" => intent_key,
        "decision" => decision,
        "reason" => "Resolved from peer gossip answers.",
        "topic" => Map.get(proposed_payload, "topic", "unknown"),
        "target_bots" => Map.get(proposed_payload, "target_bots", []),
        "summary" => Map.get(proposed_payload, "summary", "")
      }
    }

    Logger.info("[GossipCoordinator] resolved intent_key=#{intent_key} decision=#{decision}")
    BotArmyRuntime.NATS.Publisher.publish("gossip.intent.resolved", resolved)
    maybe_publish_tavern(resolved)
  end

  defp execute_and_narrate(proposed, _intent_key, decision) do
    proposed_payload = Map.get(proposed, "payload", %{})
    topic = Map.get(proposed_payload, "topic", "")
    target_bots = Map.get(proposed_payload, "target_bots", [])
    tenant_id = Map.get(proposed, "tenant_id", "00000000-0000-0000-0000-000000000001")

    if decision == "defer" do
      publish_action_narration(
        "The Keeper wipes the counter and says nothing. The matter settles like dust.",
        tenant_id,
        proposed
      )
    else
      # Pick the first target bot and query its status
      bot = List.first(target_bots, "")
      subject = resolution_status_subject(topic, bot)

      if subject != "" do
        spawn(fn ->
          case BotArmyRuntime.NATS.Publisher.request(
                 subject,
                 %{"tenant_id" => tenant_id, "schema_version" => "1.0", "limit" => 1},
                 timeout_ms: 5_000
               ) do
            {:ok, %{} = resp} ->
              count = extract_status_count(resp)
              patron = patron_for(bot)

              text =
                "#{patron} moves behind the bar. #{count} items now stand in their ledger. The Keeper notes it."

              publish_action_narration(text, tenant_id, proposed)

            _ ->
              patron = patron_for(bot)

              text =
                "#{patron} is seen in the back room, but their work has not yet reached the bar. The Keeper waits."

              publish_action_narration(text, tenant_id, proposed)
          end
        end)
      else
        publish_action_narration(
          "The Keeper glances up from the glasses. The patrons agree, but none rise. Not yet.",
          tenant_id,
          proposed
        )
      end
    end
  end

  defp resolution_status_subject(topic, bot) do
    cond do
      topic =~ "task" or topic =~ "gtd" or bot =~ "gtd" -> "gtd.task.list"
      topic =~ "calendar" or topic =~ "event" or bot =~ "calendar" -> "calendar.event.list"
      topic =~ "context" or bot =~ "context" -> "context.state.get"
      topic =~ "fitness" or bot =~ "fitness" -> "fitness.status.get"
      topic =~ "learning" or bot =~ "learning" -> "learning.progress.get"
      true -> ""
    end
  end

  defp extract_status_count(%{"data" => %{} = data}) do
    cond do
      is_list(data["tasks"]) -> "#{length(data["tasks"])}"
      is_list(data["events"]) -> "#{length(data["events"])}"
      is_binary(data["mode"]) -> data["mode"]
      is_binary(data["status"]) -> data["status"]
      true -> "Some"
    end
  end

  defp extract_status_count(%{"tasks" => tasks}) when is_list(tasks), do: "#{length(tasks)}"
  defp extract_status_count(%{"events" => events}) when is_list(events), do: "#{length(events)}"
  defp extract_status_count(_), do: "Some"

  defp patron_for("gtd_bot"), do: "The Taskmaster"
  defp patron_for("bot_army_gtd"), do: "The Taskmaster"
  defp patron_for("chore_bot"), do: "The Steward"
  defp patron_for("bot_army_chore"), do: "The Steward"
  defp patron_for("fitness_bot"), do: "The Trainer"
  defp patron_for("bot_army_fitness"), do: "The Trainer"
  defp patron_for("calendar_bot"), do: "The Timekeeper"
  defp patron_for("bot_army_calendar"), do: "The Timekeeper"
  defp patron_for("context_broker_bot"), do: "The Broker"
  defp patron_for("bot_army_context"), do: "The Broker"
  defp patron_for("llm_bot"), do: "The Oracle"
  defp patron_for("bot_army_llm"), do: "The Oracle"
  defp patron_for("synapse_bot"), do: "The Herald"
  defp patron_for("bot_army_synapse"), do: "The Herald"
  defp patron_for("learning_bot"), do: "The Sage"
  defp patron_for("bot_army_learning"), do: "The Sage"
  defp patron_for(""), do: "A patron"
  defp patron_for(bot), do: bot

  defp publish_action_narration(text, tenant_id, original_event) do
    payload = %{
      "event_id" => UUID.uuid4(),
      "event" => "gossip.tavern.narrated",
      "schema_version" => "1.0",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "tenant_id" => tenant_id,
      "conversation_id" => Map.get(original_event, "conversation_id", UUID.uuid4()),
      "payload" => %{
        "text" => text,
        "original_event" => "gossip.intent.resolved.action",
        "tavern" => true
      }
    }

    BotArmyRuntime.NATS.Publisher.publish("gossip.tavern.narrated", payload)
  end

  defp maybe_vote_on_gtd_poll(state) do
    case state.active_gtd_poll do
      nil ->
        state

      %{poll_id: poll_id} ->
        if MapSet.member?(state.voted_gtd_poll_ids, poll_id) do
          state
        else
          %{choices: choices, budget: budget, tenant_id: tenant_id} = state.active_gtd_poll

          allocations = BotArmyRuntime.GtdPollAllocator.allocate(choices, :synapse, budget)

          if allocations != [] do
            submit_gtd_vote(poll_id, allocations, tenant_id)
          end

          %{state | voted_gtd_poll_ids: MapSet.put(state.voted_gtd_poll_ids, poll_id)}
        end
    end
  end

  defp submit_gtd_vote(poll_id, allocations, tenant_id) do
    payload = %{
      "poll_id" => poll_id,
      "voter_type" => "bot",
      "voter_id" => "synapse",
      "allocations" => allocations,
      "tenant_id" => tenant_id
    }

    case BotArmyRuntime.NATS.Publisher.request("gtd.poll.vote.submit", payload, timeout_ms: 5_000) do
      {:ok, _reply} ->
        Logger.info("[GossipCoordinator] submitted GTD poll vote poll_id=#{poll_id}")

      {:error, reason} ->
        Logger.warning(
          "[GossipCoordinator] GTD poll vote failed poll_id=#{poll_id} reason=#{inspect(reason)}"
        )

        if poll_closed_error?(reason) do
          # Clear active_gtd_poll so we don't retry on closed polls
          send(self(), :clear_gtd_poll)
        end
    end
  end

  defp poll_closed_error?(reason) when is_binary(reason),
    do: String.contains?(reason, "poll_not_open")

  defp poll_closed_error?(%{"error" => _}), do: true
  defp poll_closed_error?(_), do: false

  defp publish_poll_vote(%{poll_id: poll_id, topic: topic, options: options} = poll) do
    vote = suggest_vote(topic, options, Map.get(poll, :context_snapshot, %{}))

    vote_message = %{
      "event_id" => UUID.uuid4(),
      "event" => "gossip.poll.vote",
      "schema_version" => "1.0",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "tenant_id" => BotArmyRuntime.Tenant.default_tenant_id(),
      "conversation_id" => poll_id,
      "payload" => %{
        "poll_id" => poll_id,
        "topic" => topic,
        "voter" => "synapse_bot",
        "vote" => vote,
        "reason" => "voted_on_heartbeat_wakeup"
      }
    }

    BotArmyRuntime.NATS.Publisher.publish("gossip.poll.vote", vote_message)
  end

  defp suggest_vote(topic, options, context_snapshot) do
    case topic do
      "risk" -> pick_or_fallback("quality", options)
      "focus" -> pick_or_fallback("deep_work", options)
      "coordination" -> pick_or_fallback("dependencies", options)
      "priorities" -> choose_priority_vote(options, context_snapshot)
      _ -> fallback_option(options)
    end
  end

  defp normalize_vote(vote, %{topic: topic}) do
    case {topic, vote} do
      {"priorities", "yes"} -> "protect_focus"
      {"coordination", "yes"} -> "dependencies"
      {"risk", "yes"} -> "quality"
      {"focus", "yes"} -> "deep_work"
      _ -> vote
    end
  end

  defp publish_poll_resolved(poll, winner, counts) do
    resolved = %{
      "event_id" => UUID.uuid4(),
      "event" => "gossip.poll.resolved",
      "schema_version" => "1.0",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "tenant_id" => BotArmyRuntime.Tenant.default_tenant_id(),
      "conversation_id" => poll.poll_id,
      "payload" => %{
        "poll_id" => poll.poll_id,
        "topic" => poll.topic,
        "winner" => winner,
        "counts" => counts
      }
    }

    BotArmyRuntime.NATS.Publisher.publish("gossip.poll.resolved", resolved)
    maybe_publish_tavern(resolved)
  end

  defp maybe_publish_discord_poll_summary(poll, winner, counts) do
    plain_text = "Poll '#{poll.topic}' resolved: #{winner} (#{format_counts(counts)})"

    case System.get_env("DISCORD_OPS_RAW_CHANNEL_ID") do
      channel when is_binary(channel) and channel != "" ->
        publish_discord_relay(channel, plain_text)

      _ ->
        :ok
    end
  end

  defp publish_discord_relay(channel_id, text, opts \\ []) do
    subject = "discord.relay.#{channel_id}"
    payload = Jason.encode!(Map.merge(%{"content" => text}, Map.new(opts)))

    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      {:ok, conn} -> Gnat.pub(conn, subject, payload)
      {:error, _reason} -> :ok
    end
  end

  def maybe_publish_tavern(event) when is_map(event) do
    # Tavern narration not available in lite mode
    :ok
  end

  def maybe_publish_tavern_full(event, _flavored) when is_map(event) do
      nil ->
        :ok

      flavored ->
        publish_tavern_nats(event, flavored)
    end
  end

  @tavern_dedup_ttl_s 60

  defp publish_tavern_nats(original_event, flavored_text) do
    event = Map.get(original_event, "event", "unknown")

    bot_id =
      get_in(original_event, ["payload", "bot_id"]) ||
        get_in(original_event, ["payload", "service"]) ||
        get_in(original_event, ["payload", "intent_key"]) ||
        get_in(original_event, ["payload", "poll_id"]) ||
        "unknown"

    dedup_key = {:tavern_dedup, event, bot_id}
    now = System.system_time(:second)

    case :ets.lookup(@table, dedup_key) do
      [{_, last_sent}] when last_sent > now - @tavern_dedup_ttl_s ->
        :ok

      _ ->
        :ets.insert(@table, {dedup_key, now})

        payload = %{
          "event_id" => UUID.uuid4(),
          "event" => "gossip.tavern.narrated",
          "schema_version" => "1.0",
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
          "source" => "bot_army_synapse",
          "tenant_id" =>
            Map.get(original_event, "tenant_id", "00000000-0000-0000-0000-000000000001"),
          "conversation_id" => Map.get(original_event, "conversation_id", UUID.uuid4()),
          "payload" => %{
            "text" => flavored_text,
            "original_event" => Map.get(original_event, "event", "unknown"),
            "tavern" => true
          }
        }

        BotArmyRuntime.NATS.Publisher.publish("gossip.tavern.narrated", payload)
    end
  end

  defp tally_votes(votes) do
    counts =
      votes
      |> Map.values()
      |> Enum.reduce(%{}, fn vote, acc -> Map.update(acc, vote, 1, &(&1 + 1)) end)

    winner =
      case Enum.max_by(counts, fn {_k, v} -> v end, fn -> {"no_votes", 0} end) do
        {k, _v} -> k
      end

    {winner, counts}
  end

  defp format_counts(counts) do
    counts
    |> Enum.map(fn {k, v} -> "#{k}=#{v}" end)
    |> Enum.sort()
    |> Enum.join(", ")
  end

  defp choose_priority_vote(options, context_snapshot) do
    BotArmyRuntime.GossipPollAffinity.choose_priority_vote(
      options,
      context_snapshot,
      :synapse,
      ""
    )
  end

  defp pick_or_fallback(preferred, options) do
    if preferred in options, do: preferred, else: fallback_option(options)
  end

  defp fallback_option(options) when is_list(options) and options != [], do: List.first(options)
  defp fallback_option(_), do: "upvote"
end
