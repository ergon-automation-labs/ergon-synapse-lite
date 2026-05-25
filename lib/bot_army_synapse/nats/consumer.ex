defmodule BotArmySynapse.NATS.Consumer do
  @moduledoc """
  NATS message consumer for the Synapse Lite service.

  Subscribes to NATS subjects for RAG orchestration and decision engine:
  - `llm.context.analyze` - Main orchestration entry point
  - `events.gtd.task.>` - Listen for task updates
  - `events.gtd.project.>` - Listen for project updates
  - `events.calendar.>` - Listen for calendar updates
  - `events.context.state.changed` - Context mode changes
  - `events.sre.alert.>` - SRE alert events
  - `bot_army.claude.result.>` - Claude Bridge session results
  - `synapse.log.create` - Create log entry (request/reply)
  - `synapse.task.create` - Create GTD task (request/reply)
  - `synapse.task.list` - List GTD tasks (request/reply)

  Routes messages to appropriate handlers for context gathering,
  LLM prompt formatting, and decision engine evaluation.
  """

  use GenServer
  require Logger

  alias BotArmyRuntime.NATS.Reply
  @registry_heartbeat_ms 20_000
  @version Mix.Project.config()[:version]

  @subjects [
    %{subject: "synapse.log.create", type: :request_reply, description: "Create log entry"},
    %{subject: "synapse.task.create", type: :request_reply, description: "Create GTD task"},
    %{subject: "synapse.task.list", type: :request_reply, description: "List GTD tasks"},
    %{subject: "synapse.query", type: :request_reply, description: "Query context"},
    %{subject: "synapse.analyze", type: :request_reply, description: "Analyze context"},
    %{subject: "synapse.continue", type: :request_reply, description: "Continue prior run"},
    %{subject: "synapse.run.get", type: :request_reply, description: "Get run state by run_id"},
    %{subject: "synapse.run.list", type: :request_reply, description: "List recent runs"},
    %{subject: "synapse.workflow", type: :request_reply, description: "Execute workflow"},
    %{subject: "synapse.feedback", type: :request_reply, description: "Store feedback"},
    %{
      subject: "daily.command.recommendation.current",
      type: :request_reply,
      description: "Get latest daily command recommendation"
    },
    %{
      subject: "daily.review.start",
      type: :request_reply,
      description: "Start morning/evening daily review question flow"
    },
    %{
      subject: "daily.review.submit",
      type: :request_reply,
      description: "Submit daily review answers and generate next actions"
    },
    %{subject: "synapse.goal.list", type: :request_reply, description: "List goals"},
    %{subject: "synapse.goal.get", type: :request_reply, description: "Get goal"},
    %{subject: "synapse.goal.update", type: :request_reply, description: "Update goal"},
    %{subject: "synapse.goal.progress", type: :request_reply, description: "Get goal progress"},
    %{
      subject: "gossip.intent.proposed",
      type: :subscribe,
      description: "Receive gossip intent proposals"
    },
    %{
      subject: "gossip.intent.answer",
      type: :subscribe,
      description: "Receive gossip peer answers"
    },
    %{
      subject: "gossip.social.reply",
      type: :subscribe,
      description: "Receive social gossip replies"
    },
    %{
      subject: "gossip.poll.broadcast",
      type: :subscribe,
      description: "Receive army general poll broadcasts"
    },
    %{
      subject: "gossip.poll.vote",
      type: :subscribe,
      description: "Receive poll votes from waking bots"
    },
    %{
      subject: "events.bot_army.intent.>",
      type: :subscribe,
      description: "Intent lifecycle events (proposed, vetoed, deferred, acted, aborted)"
    },
    %{
      subject: "bot_army.synapse.intent.proactive_message",
      type: :subscribe,
      description: "Intent: proactive contextual message"
    },
    %{
      subject: "memory.user.recall",
      type: :request_reply,
      description: "Recall last N events for a user"
    },
    %{
      subject: "memory.user.summary",
      type: :request_reply,
      description: "Summary of recent signals for a user"
    },
    %{
      subject: "bot_army.outcome.feedback",
      type: :subscribe,
      description: "Outcome feedback from surfaces for context ROI tracking"
    }
  ]

  # API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Callbacks

  @impl true
  def init(opts) do
    Logger.info("Starting NATS consumer")

    state = %{
      subscriptions: [],
      reconnect_attempt: 0,
      opts: opts,
      registry_registered?: false
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      {:ok, conn} ->
        BotArmyRuntime.NATS.Connection.subscribe_to_status()
        subscribe_to_topics(conn, state)

      {:error, _reason} ->
        handle_connection_unavailable(state)
    end
  end

  defp subscribe_to_topics(conn, state) do
    Logger.info("Connected to NATS, subscribing to NATS topics")

    subjects = [
      "llm.context.analyze",
      "discord.chat",
      "discord.command.>",
      "events.gtd.task.>",
      "events.gtd.project.>",
      "events.calendar.>",
      "events.context.state.changed",
      "events.sre.alert.>",
      "bot_army.claude.result.>",
      "bot.army.health.stale",
      "bot.army.health.recovered",
      "synapse.log.create",
      "synapse.task.create",
      "synapse.task.list",
      "synapse.goal.list",
      "synapse.goal.get",
      "synapse.goal.update",
      "synapse.goal.progress",
      "synapse.query",
      "synapse.analyze",
      "synapse.continue",
      "synapse.run.get",
      "synapse.run.list",
      "synapse.workflow",
      "synapse.feedback",
      "daily.command.recommendation.current",
      "daily.command.override",
      "daily.review.start",
      "daily.review.submit",
      "events.synapse.run.progress",
      "gossip.intent.proposed",
      "gossip.intent.answer",
      "gossip.social.reply",
      "gossip.poll.broadcast",
      "gossip.poll.vote",
      "events.bot_army.intent.>",
      "memory.user.recall",
      "memory.user.summary",
      "bot_army.outcome.feedback"
    ]

    subs =
      Enum.reduce_while(subjects, [], fn subject, acc ->
        case Gnat.sub(conn, self(), subject) do
          {:ok, sub} ->
            Logger.info("NATS consumer subscribed to #{subject}")
            {:cont, [sub | acc]}

          {:error, reason} ->
            Logger.error("Failed to subscribe to #{subject}: #{inspect(reason)}")
            {:halt, acc}
        end
      end)

    case subs do
      subs when length(subs) == length(subjects) ->
        BotArmyRuntime.Registry.register("synapse", @subjects, @version)
        Process.send_after(self(), :registry_heartbeat, @registry_heartbeat_ms)
        {:noreply, %{state | subscriptions: subs, registry_registered?: true}}

      _ ->
        Logger.error("Failed to subscribe to all NATS topics")
        Process.send_after(self(), :reconnect, 5000)
        {:noreply, state}
    end
  end

  defp handle_connection_unavailable(state) do
    Logger.warning("NATS connection not ready, will retry")
    Process.send_after(self(), :connect_retry, 5000)
    {:noreply, state}
  end

  @impl true
  def handle_info(:connect_retry, state) do
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info({:msg, msg}, state) do
    BotArmyRuntime.Tracing.with_consumer_span(msg.topic, Map.get(msg, :headers, []), fn ->
      Logger.debug("NATS received NATS message on subject: #{msg.topic}")

      cond do
        String.starts_with?(msg.topic, "gossip.") ->
          handle_gossip_message(msg)

        # Synapse API request/reply subjects
        String.starts_with?(msg.topic, "synapse.") ->
          handle_synapse_api(msg)

        # Discord reaction events
        msg.topic == "bot_army.outcome.feedback" ->
          case Jason.decode(msg.body) do
            {:ok, decoded} ->
              BotArmySynapse.OutcomeFeedbackStore.record(decoded)
              BotArmySynapse.ReflectionEngine.match_feedback(decoded)

            {:error, reason} ->
              Logger.warning(
                "[Consumer] Failed to decode bot_army.outcome.feedback: #{inspect(reason)}"
              )
          end

        # Discord messages from surface_discord use plain JSON, not the envelope format.
        String.starts_with?(msg.topic, "discord.") ->
          Logger.debug(
            "[Consumer] Received discord message on #{msg.topic}, reply_to=#{inspect(msg.reply_to)}"
          )

          case Jason.decode(msg.body) do
            {:ok, decoded} ->
              Logger.debug("[Consumer] Decoded discord message: #{inspect(decoded, limit: 5)}")
              BotArmySynapse.Orchestrator.handle_discord_message(decoded, msg.topic, msg.reply_to)

            {:error, reason} ->
              Logger.warning("Failed to decode Discord message: #{inspect(reason)}")

              if msg.reply_to do
                send_discord_reply(msg.reply_to, "Sorry, I couldn't process that message.")
              end
          end

        # Subjects not handled in lite mode
        msg.topic in ["rpg.action.player", "synapse.army.opinion.vote", "discord.react.add"] or
          String.starts_with?(msg.topic, "synapse.notifications.") or
            String.starts_with?(msg.topic, "factory.") ->
          Logger.warning("Subject #{msg.topic} not handled in synapse-lite mode")

        true ->
          case BotArmyCore.NATS.Decoder.decode(msg.body) do
            {:ok, decoded} ->
              # Include reply_to from NATS message for request/reply patterns
              message_with_reply = Map.put(decoded, "reply_to", msg.reply_to)
              route_message(message_with_reply, msg.topic)

            {:error, reason} ->
              Logger.warning("Failed to decode message from #{msg.topic}: #{inspect(reason)}")
          end
      end
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info(:reconnect, state) do
    Logger.info("Attempting to reconnect to NATS")
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info({:nats, :disconnected}, state) do
    Logger.warning("Disconnected from NATS, will reconnect")
    Process.send_after(self(), :reconnect, 5000)
    {:noreply, %{state | subscriptions: []}}
  end

  @impl true
  def handle_info({:nats, :connected}, state) do
    Logger.info("Reconnected to NATS, re-subscribing")
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info(:registry_heartbeat, state) do
    if state.registry_registered? do
      BotArmyRuntime.Registry.register("synapse", @subjects, @version)
      BotArmySynapse.GossipCoordinator.maybe_vote_on_heartbeat()
      Process.send_after(self(), :registry_heartbeat, @registry_heartbeat_ms)
    end

    {:noreply, state}
  end

  # Message routing

  defp route_message(message, subject) do
    event = message["event"]
    payload = message["payload"] || %{}

    cond do
      subject == "llm.context.analyze" ->
        BotArmySynapse.Orchestrator.handle_analyze(message)

      subject == "daily.command.recommendation.current" ->
        BotArmySynapse.Orchestrator.handle_daily_command_recommendation_current(message)

      subject == "daily.command.override" ->
        BotArmySynapse.Orchestrator.handle_daily_command_override(message)

      subject == "daily.review.start" ->
        BotArmySynapse.Orchestrator.handle_daily_review_start(message)

      subject == "daily.review.submit" ->
        BotArmySynapse.Orchestrator.handle_daily_review_submit(message)

      String.starts_with?(subject, "events.gtd.task.") ->
        BotArmySynapse.Context.GTD.handle_event(message, event)
        record_event_from_message(subject, event, payload)
        evaluate_decision(:gtd_task_updated, message)

        if event in ["gtd.task.created", "gtd.task.completed"] do
          BotArmySynapse.GossipCoordinator.maybe_publish_tavern(message)
        end

      String.starts_with?(subject, "events.gtd.project.") ->
        BotArmySynapse.Context.Goals.handle_event(message, event)
        record_event_from_message(subject, :gtd_project_updated, payload)

      String.starts_with?(subject, "events.calendar.") ->
        BotArmySynapse.Context.Calendar.handle_event(message, event)
        record_event_from_message(subject, event, payload)
        evaluate_decision(:calendar_event, message)

      subject == "events.context.state.changed" ->
        BotArmySynapse.Context.State.handle_update(message)
        record_event_from_message(subject, :context_mode_changed, payload)
        evaluate_decision(:context_state_changed, message)

      String.starts_with?(subject, "events.sre.alert.") ->
        BotArmySynapse.Context.SRE.handle_event(message, event)
        record_event_from_message(subject, :alert_fired, payload)
        evaluate_decision(:sre_alert, message)
        BotArmySynapse.GossipCoordinator.maybe_publish_tavern(message)

      String.starts_with?(subject, "bot_army.claude.result.") ->
        BotArmySynapse.Orchestrator.handle_claude_result(message)

      String.starts_with?(subject, "bot.army.health.") ->
        BotArmySynapse.Context.Fleet.handle_event(message, event)
        record_event_from_message(subject, :fleet_health, payload)
        BotArmySynapse.GossipCoordinator.maybe_publish_tavern(message)

      String.starts_with?(subject, "discord.command.") ->
        reply_to = message["reply_to"]
        BotArmySynapse.Orchestrator.handle_discord_message(message, subject, reply_to)

      subject == "events.synapse.run.progress" ->
        BotArmySynapse.SynapseProgressHandler.handle_event(message, subject)

      String.starts_with?(subject, "events.bot_army.intent.") ->
        BotArmySynapse.IntentEventHandler.handle(message, subject)

      # Subjects not handled in lite mode
      subject in [
        "factory.vote.recorded",
        "factory.evidence.breaker",
        "gtd.poll.created",
        "gtd.poll.closed",
        "poll.start",
        "poll.vote.submit",
        "poll.get",
        "poll.close"
      ] or
        String.starts_with?(subject, "events.job.listings.") or
        String.starts_with?(subject, "events.fitness.") or
        String.starts_with?(subject, "events.chore.") or
        String.starts_with?(subject, "events.terrain.") or
        String.starts_with?(subject, "pi-go.event.") or
          String.starts_with?(subject, "pi_go.event.") ->
        Logger.warning("Subject #{subject} not handled in synapse-lite mode")

      true ->
        Logger.debug("Unknown NATS message event: #{event} on subject: #{subject}")
    end
  end

  defp handle_gossip_message(msg) do
    case Jason.decode(msg.body) do
      {:ok, decoded} ->
        case msg.topic do
          "gossip.intent.proposed" ->
            BotArmySynapse.GossipCoordinator.record_intent_proposed(decoded)

          "gossip.intent.answer" ->
            BotArmySynapse.GossipCoordinator.record_intent_answer(decoded)

          "gossip.social.reply" ->
            BotArmySynapse.GossipCoordinator.record_social_reply(decoded)

          "gossip.poll.broadcast" ->
            BotArmySynapse.GossipCoordinator.record_poll_broadcast(decoded)

          "gossip.poll.vote" ->
            BotArmySynapse.GossipCoordinator.record_poll_vote(decoded)

          _ ->
            Logger.debug("[Synapse Gossip] Ignoring topic #{msg.topic}")
        end

      {:error, reason} ->
        Logger.warning("[Synapse Gossip] Failed to decode #{msg.topic}: #{inspect(reason)}")
    end
  end

  defp handle_memory_recall(msg) do
    case Jason.decode(msg.body) do
      {:ok, decoded} ->
        payload = Map.get(decoded, "payload", decoded)
        tenant_id = Map.get(payload, "tenant_id", default_tenant_id())
        user_id = Map.get(payload, "user_id")
        limit = Map.get(payload, "limit", 20)

        events = BotArmySynapse.MemoryBroker.recall(tenant_id, user_id, limit)

        if msg.reply_to do
          reply_nats(msg.reply_to, BotArmyRuntime.NATS.Reply.ok(%{"events" => events}))
        end

      {:error, reason} ->
        Logger.warning("Failed to decode memory.user.recall: #{inspect(reason)}")

        if msg.reply_to do
          reply_nats(
            msg.reply_to,
            BotArmyRuntime.NATS.Reply.error("decode_failed", :decode_error)
          )
        end
    end
  end

  defp handle_memory_summary(msg) do
    case Jason.decode(msg.body) do
      {:ok, decoded} ->
        payload = Map.get(decoded, "payload", decoded)
        tenant_id = Map.get(payload, "tenant_id", default_tenant_id())
        user_id = Map.get(payload, "user_id")

        summary = BotArmySynapse.MemoryBroker.summary(tenant_id, user_id)

        if msg.reply_to do
          reply_nats(msg.reply_to, BotArmyRuntime.NATS.Reply.ok(summary))
        end

      {:error, reason} ->
        Logger.warning("Failed to decode memory.user.summary: #{inspect(reason)}")

        if msg.reply_to do
          reply_nats(
            msg.reply_to,
            BotArmyRuntime.NATS.Reply.error("decode_failed", :decode_error)
          )
        end
    end
  end

  defp record_event_from_message(subject, event_type, payload) when is_atom(event_type) do
    summary = build_event_summary(event_type, payload)
    BotArmySynapse.EventHistory.record_event(event_type, summary)

    # Persist to knowledge graph
    BotArmySynapse.Stores.KnowledgeStore.create_event(%{
      event_type: to_string(event_type),
      summary: summary,
      tenant_id: default_tenant_id(),
      metadata: payload,
      occurred_at: DateTime.utc_now()
    })

    maybe_record_memory(subject, to_string(event_type), payload)
  end

  defp record_event_from_message(subject, event_type, payload) when is_binary(event_type) do
    atom_type = String.to_atom(event_type)
    summary = build_event_summary(atom_type, payload)
    BotArmySynapse.EventHistory.record_event(atom_type, summary)

    # Persist to knowledge graph
    BotArmySynapse.Stores.KnowledgeStore.create_event(%{
      event_type: event_type,
      summary: summary,
      tenant_id: default_tenant_id(),
      metadata: payload,
      occurred_at: DateTime.utc_now()
    })

    maybe_record_memory(subject, event_type, payload)
  end

  defp maybe_record_memory(_subject, event_type, payload) do
    if event_type in BotArmySynapse.MemoryBroker.tracked_event_types() do
      tenant_id = Map.get(payload, "tenant_id", default_tenant_id())
      user_id = Map.get(payload, "user_id")
      BotArmySynapse.MemoryBroker.record_event(tenant_id, user_id, event_type, payload)
    end
  end

  defp build_event_summary(:gtd_task_created, payload),
    do: "Task created: #{Map.get(payload, "title", "unknown")}"

  defp build_event_summary(:gtd_task_completed, payload),
    do: "Task completed: #{Map.get(payload, "title", "unknown")}"

  defp build_event_summary(:listing_added, payload),
    do: "New job listing: #{Map.get(payload, "title", Map.get(payload, "company", "unknown"))}"

  defp build_event_summary(:calendar_event, payload),
    do: "Calendar: #{Map.get(payload, "summary", "event")}"

  defp build_event_summary(:context_mode_changed, payload),
    do: "Context mode → #{Map.get(payload, "mode", "unknown")}"

  defp build_event_summary(:alert_fired, payload),
    do: "SRE alert: #{Map.get(payload, "alert_name", Map.get(payload, "name", "unknown"))}"

  defp build_event_summary(:game_generated, payload),
    do: "Terrain game generated: #{Map.get(payload, "track", "unknown")}"

  defp build_event_summary(:chore_overdue, payload),
    do: "Chore overdue: #{Map.get(payload, "name", "unknown")}"

  defp build_event_summary(:fleet_health, payload),
    do: "Fleet health: #{Map.get(payload, "bot_id", "unknown")}"

  defp build_event_summary(event_type, _payload), do: "#{event_type}"

  # Synapse API request/reply handlers

  defp handle_synapse_api(msg) do
    case msg.topic do
      "synapse.log.create" ->
        handle_log_create(msg)

      "synapse.task.create" ->
        handle_task_create(msg)

      "synapse.task.list" ->
        handle_task_list(msg)

      "synapse.query" ->
        handle_knowledge_query(msg)

      "synapse.analyze" ->
        handle_llm_analyze(msg)

      "synapse.continue" ->
        handle_continue_run(msg)

      "synapse.run.get" ->
        handle_run_get(msg)

      "synapse.run.list" ->
        handle_run_list(msg)

      "synapse.workflow" ->
        handle_workflow(msg)

      "synapse.feedback" ->
        handle_feedback(msg)

      "synapse.goal.list" ->
        handle_goal_list(msg)

      "synapse.goal.get" ->
        handle_goal_get(msg)

      "synapse.goal.update" ->
        handle_goal_update(msg)

      "synapse.goal.progress" ->
        handle_goal_progress(msg)

      "memory.user.recall" ->
        handle_memory_recall(msg)

      "memory.user.summary" ->
        handle_memory_summary(msg)

      subject
      when subject in [
             "synapse.task.automate_now",
             "synapse.campaign.resume",
             "synapse.poll.start",
             "synapse.poll.close",
             "synapse.whats_next",
             "synapse.army_general.poll.broadcast"
           ] ->
        Logger.warning("Subject #{subject} not handled in synapse-lite mode")

        if msg.reply_to do
          reply_nats(
            msg.reply_to,
            BotArmyRuntime.NATS.Reply.error("not_available_in_lite_mode", :not_implemented)
          )
        end

      other ->
        Logger.warning("Unknown synapse API subject: #{other}")

        if msg.reply_to do
          reply_nats(
            msg.reply_to,
            BotArmyRuntime.NATS.Reply.error("unknown subject", :unknown_subject)
          )
        end
    end
  end

  defp handle_log_create(msg) do
    case Jason.decode(msg.body) do
      {:ok, decoded} ->
        message =
          Map.get(decoded, "message", Map.get(decoded, "payload", %{}) |> Map.get("message", ""))

        event_type = Map.get(decoded, "event_type", :log_entry)

        BotArmySynapse.EventHistory.record_event(
          if(is_binary(event_type), do: String.to_atom(event_type), else: event_type),
          message
        )

        if msg.reply_to do
          reply_nats(msg.reply_to, BotArmyRuntime.NATS.Reply.ok(%{"recorded" => true}))
        end

      {:error, reason} ->
        Logger.warning("Failed to decode synapse.log.create: #{inspect(reason)}")

        if msg.reply_to do
          reply_nats(
            msg.reply_to,
            BotArmyRuntime.NATS.Reply.error("decode_failed", :decode_error)
          )
        end
    end
  end

  defp handle_task_create(msg) do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000),
         {:ok, decoded} <- Jason.decode(msg.body) do
      payload = Map.get(decoded, "payload", decoded)
      title = Map.get(payload, "title", "Untitled task")
      context = Map.get(payload, "context", "inbox")
      priority = Map.get(payload, "priority", "normal")
      description = Map.get(payload, "description")
      labels = Map.get(payload, "labels", [])
      project_id = Map.get(payload, "project_id")
      goal_id = Map.get(payload, "goal_id")
      parent_task_id = Map.get(payload, "parent_task_id")

      bridge_payload =
        %{
          "title" => title,
          "context" => context,
          "priority" => priority
        }
        |> then(fn p -> if description, do: Map.put(p, "description", description), else: p end)
        |> then(fn p -> if labels != [], do: Map.put(p, "labels", labels), else: p end)
        |> then(fn p -> if project_id, do: Map.put(p, "project_id", project_id), else: p end)
        |> then(fn p -> if goal_id, do: Map.put(p, "goal_id", goal_id), else: p end)
        |> then(fn p ->
          if parent_task_id, do: Map.put(p, "parent_task_id", parent_task_id), else: p
        end)

      case Gnat.request(conn, "bridge.task.create", Jason.encode!(bridge_payload),
             receive_timeout: 5000
           ) do
        {:ok, response} ->
          if msg.reply_to do
            body = extract_nats_body(response)
            reply_nats(msg.reply_to, body)
          end

        {:error, _reason} ->
          if msg.reply_to do
            reply_nats(
              msg.reply_to,
              Reply.error("bridge_request_failed", :upstream_error)
            )
          end

        {:timeout, _} ->
          if msg.reply_to do
            reply_nats(msg.reply_to, Reply.error("bridge_timeout", :timeout))
          end
      end
    else
      {:error, _reason} ->
        Logger.warning("synapse.task.create failed")

        if msg.reply_to do
          reply_nats(msg.reply_to, Reply.error("connection_failed", :connection_error))
        end
    end
  end

  defp handle_task_list(msg) do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      case Gnat.request(conn, "gtd.task.list", Jason.encode!(%{}), receive_timeout: 5000) do
        {:ok, response} ->
          if msg.reply_to do
            body = extract_nats_body(response)
            reply_nats(msg.reply_to, body)
          end

        {:error, _reason} ->
          if msg.reply_to do
            reply_nats(
              msg.reply_to,
              Reply.error("gtd_request_failed", :upstream_error)
            )
          end

        {:timeout, _} ->
          if msg.reply_to do
            reply_nats(msg.reply_to, Reply.error("gtd_timeout", :timeout))
          end
      end
    else
      {:error, _reason} ->
        Logger.warning("synapse.task.list failed")

        if msg.reply_to do
          reply_nats(msg.reply_to, Reply.error("connection_failed", :connection_error))
        end
    end
  end

  defp handle_knowledge_query(msg) do
    case Jason.decode(msg.body) do
      {:ok, payload} ->
        tenant_id = Map.get(payload, "tenant_id", default_tenant_id())
        query = Map.get(payload, "query", "")

        opts = [
          limit: Map.get(payload, "limit", 50),
          from: Map.get(payload, "from"),
          to: Map.get(payload, "to"),
          event_type: Map.get(payload, "event_type")
        ]

        result = BotArmySynapse.Stores.KnowledgeStore.query(tenant_id, query, opts)

        if msg.reply_to do
          reply_nats(
            msg.reply_to,
            Reply.ok(%{
              "events" => serialize_events(result.events),
              "notes" => serialize_notes(result.notes)
            })
          )
        end

      {:error, reason} ->
        Logger.warning("Failed to decode synapse.query: #{inspect(reason)}")

        if msg.reply_to do
          reply_nats(
            msg.reply_to,
            Reply.error("decode_failed", :decode_error)
          )
        end
    end
  end

  defp serialize_events(events) do
    Enum.map(events, fn e ->
      %{
        "id" => e.id,
        "event_type" => e.event_type,
        "summary" => e.summary,
        "metadata" => e.metadata,
        "occurred_at" => e.occurred_at
      }
    end)
  end

  defp serialize_notes(notes) do
    Enum.map(notes, fn n ->
      %{
        "id" => n.id,
        "content" => n.content,
        "tags" => n.tags,
        "inserted_at" => n.inserted_at
      }
    end)
  end

  defp default_tenant_id,
    do: System.get_env("BOT_ARMY_TENANT_ID", "00000000-0000-0000-0000-000000000001")

  defp handle_llm_analyze(msg) do
    case Jason.decode(msg.body) do
      {:ok, payload} ->
        event_id = Map.get(payload, "event_id", UUID.uuid4())

        spawn(fn ->
          BotArmySynapse.Orchestrator.handle_llm_analyze(payload, event_id)
        end)

      {:error, reason} ->
        Logger.warning("Failed to decode synapse.analyze: #{inspect(reason)}")
    end
  end

  defp handle_workflow(msg) do
    case Jason.decode(msg.body) do
      {:ok, payload} ->
        reply_to = Map.get(msg, :reply_to)

        spawn(fn ->
          BotArmySynapse.Orchestrator.dispatch_workflow(payload, reply_to)
        end)

      {:error, reason} ->
        Logger.warning("Failed to decode synapse.workflow: #{inspect(reason)}")

        if Map.get(msg, :reply_to) do
          reply_nats(msg.reply_to, Reply.error("decode_failed", :decode_error))
        end
    end
  end

  defp handle_continue_run(msg) do
    case Jason.decode(msg.body) do
      {:ok, payload} ->
        request = Map.get(payload, "payload", payload)
        reply_to = Map.get(msg, :reply_to)
        run_id = Map.get(request, "run_id", "")
        request_tenant_id = Map.get(request, "tenant_id", default_tenant_id())

        case BotArmySynapse.RunStore.get_run(run_id) do
          nil ->
            if reply_to do
              reply_nats(reply_to, Reply.error("run_not_found", :not_found))
            end

          run ->
            run_tenant_id = Map.get(run, "tenant_id")

            if tenant_allowed?(request_tenant_id, run_tenant_id) do
              spawn(fn ->
                BotArmySynapse.Orchestrator.handle_continue(payload, reply_to)
              end)
            else
              if reply_to do
                reply_nats(reply_to, Reply.error("tenant_mismatch", :forbidden))
              end
            end
        end

      {:error, reason} ->
        Logger.warning("Failed to decode synapse.continue: #{inspect(reason)}")

        if Map.get(msg, :reply_to) do
          reply_nats(msg.reply_to, Reply.error("decode_failed", :decode_error))
        end
    end
  end

  defp handle_run_get(msg) do
    case Jason.decode(msg.body) do
      {:ok, payload} ->
        request = Map.get(payload, "payload", payload)
        run_id = Map.get(request, "run_id", "")
        tenant_id = Map.get(request, "tenant_id", default_tenant_id())

        response =
          case BotArmySynapse.RunStore.get_run(run_id) do
            nil ->
              Reply.error("run_not_found", :not_found)

            run ->
              if tenant_allowed?(tenant_id, Map.get(run, "tenant_id")) do
                Reply.ok(%{"run" => run})
              else
                Reply.error("tenant_mismatch", :forbidden)
              end
          end

        if msg.reply_to do
          reply_nats(msg.reply_to, response)
        end

      {:error, reason} ->
        Logger.warning("Failed to decode synapse.run.get: #{inspect(reason)}")

        if Map.get(msg, :reply_to) do
          reply_nats(msg.reply_to, Reply.error("decode_failed", :decode_error))
        end
    end
  end

  defp handle_run_list(msg) do
    case Jason.decode(msg.body) do
      {:ok, payload} ->
        request = Map.get(payload, "payload", payload)
        tenant_id = Map.get(request, "tenant_id", default_tenant_id())
        limit = parse_limit(Map.get(request, "limit", 50))
        runs = BotArmySynapse.RunStore.list_runs(limit, tenant_id)

        if msg.reply_to do
          reply_nats(msg.reply_to, Reply.ok(%{"runs" => runs, "count" => length(runs)}))
        end

      {:error, reason} ->
        Logger.warning("Failed to decode synapse.run.list: #{inspect(reason)}")

        if Map.get(msg, :reply_to) do
          reply_nats(msg.reply_to, Reply.error("decode_failed", :decode_error))
        end
    end
  end

  defp handle_feedback(msg) do
    case Jason.decode(msg.body) do
      {:ok, payload} ->
        spawn(fn ->
          BotArmySynapse.Orchestrator.handle_feedback(payload)
        end)

      {:error, reason} ->
        Logger.warning("Failed to decode synapse.feedback: #{inspect(reason)}")
    end
  end

  defp handle_goal_list(msg) do
    goals = BotArmySynapse.GoalStore.list_goals(:all) || []
    task_counts = fetch_task_counts_by_project_id()
    goals_with_counts = enrich_goals_with_task_counts(goals, task_counts)

    if msg.reply_to do
      reply_nats(
        msg.reply_to,
        Reply.ok(%{"goals" => goals_with_counts, "count" => length(goals_with_counts)})
      )
    end
  end

  defp fetch_task_counts_by_project_id do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000),
         {:ok, tasks} <- fetch_all_tasks(conn, 500, 0, []) do
      count_tasks_by_project(tasks)
    else
      _ -> %{}
    end
  end

  defp fetch_all_tasks(conn, limit, offset, acc) do
    envelope =
      BotArmySynapse.build_envelope("gtd.task.list", %{
        "limit" => limit,
        "offset" => offset
      })

    case Gnat.request(conn, "gtd.task.list", Jason.encode!(envelope), receive_timeout: 5000) do
      {:ok, response} ->
        with {:ok, decoded} <- Jason.decode(extract_nats_body(response)),
             data <- Map.get(decoded, "data", %{}),
             tasks <- Map.get(data, "tasks", []),
             total_count <- Map.get(data, "total_count", length(tasks)) do
          all_tasks = acc ++ tasks
          next_offset = offset + length(tasks)

          cond do
            tasks == [] -> {:ok, all_tasks}
            next_offset >= total_count -> {:ok, all_tasks}
            true -> fetch_all_tasks(conn, limit, next_offset, all_tasks)
          end
        else
          _ -> {:error, :invalid_task_list_response}
        end

      {:error, reason} ->
        {:error, reason}

      {:timeout, _} ->
        {:error, :timeout}
    end
  end

  defp enrich_goals_with_task_counts(goals, task_counts) do
    Enum.map(goals, fn goal ->
      project_id = Map.get(goal, "id")
      Map.put(goal, "task_count", Map.get(task_counts, project_id, 0))
    end)
  end

  def count_tasks_by_project(tasks) when is_list(tasks) do
    Enum.reduce(tasks, %{}, fn task, acc ->
      case Map.get(task, "project_id") do
        project_id when is_binary(project_id) and byte_size(project_id) > 0 ->
          Map.update(acc, project_id, 1, &(&1 + 1))

        _ ->
          acc
      end
    end)
  end

  defp handle_goal_get(msg) do
    case Jason.decode(msg.body) do
      {:ok, %{"project_id" => project_id}} ->
        goal = BotArmySynapse.GoalStore.get_goal(project_id)

        if msg.reply_to do
          reply_nats(
            msg.reply_to,
            Reply.ok(%{"goal" => goal, "found" => !is_nil(goal)})
          )
        end

      {:ok, %{"name" => name}} ->
        goal = BotArmySynapse.GoalStore.get_goal_by_name(name)

        if msg.reply_to do
          reply_nats(
            msg.reply_to,
            Reply.ok(%{"goal" => goal, "found" => !is_nil(goal)})
          )
        end

      {:error, _} ->
        if msg.reply_to do
          reply_nats(msg.reply_to, Reply.error("invalid_json", :invalid_json))
        end
    end
  end

  defp handle_goal_update(msg) do
    case Jason.decode(msg.body) do
      {:ok, %{"project_id" => project_id} = payload} ->
        summary = Map.get(payload, "progress_summary", "")
        BotArmySynapse.GoalStore.update_progress(project_id, summary)

        if msg.reply_to do
          reply_nats(msg.reply_to, Reply.ok(%{"updated" => true}))
        end

      {:error, _} ->
        if msg.reply_to do
          reply_nats(msg.reply_to, Reply.error("invalid_json", :invalid_json))
        end
    end
  end

  defp handle_goal_progress(msg) do
    case Jason.decode(msg.body) do
      {:ok, %{"project_id" => project_id, "summary" => summary}} ->
        BotArmySynapse.GoalStore.update_progress(project_id, summary)

        if msg.reply_to do
          reply_nats(msg.reply_to, Reply.ok(%{"updated" => true}))
        end

      {:error, _} ->
        if msg.reply_to do
          reply_nats(msg.reply_to, Reply.error("invalid_json", :invalid_json))
        end
    end
  end

  defp reply_nats(reply_to, body) do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      headers = BotArmyRuntime.Tracing.inject_trace_context([])
      Gnat.pub(conn, reply_to, body, headers: headers)
    end
  end

  defp extract_nats_body(%{body: body}), do: body
  defp extract_nats_body(body) when is_binary(body), do: body

  defp evaluate_decision(event_type, message) do
    payload = message["payload"] || %{}

    context_data =
      case event_type do
        :gtd_task_updated ->
          %{gtd_tasks: BotArmySynapse.Context.GTD.get_context()}

        :calendar_event ->
          %{calendar_events: BotArmySynapse.Context.Calendar.get_context()}

        :context_state_changed ->
          %{context_state: payload}

        :sre_alert ->
          %{sre_alerts: BotArmySynapse.Context.SRE.get_context()}

        _ ->
          %{}
      end

    BotArmySynapse.DecisionEngine.evaluate(event_type, context_data)
  end

  defp send_discord_reply(reply_to, text) do
    response = Jason.encode!(%{"response" => text})

    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      Gnat.pub(conn, reply_to, response)
    end
  end

  defp parse_limit(limit) when is_integer(limit) and limit > 0 and limit <= 200, do: limit

  defp parse_limit(limit) when is_binary(limit) do
    case Integer.parse(limit) do
      {value, ""} -> parse_limit(value)
      _ -> 50
    end
  end

  defp parse_limit(_), do: 50

  defp tenant_allowed?(request_tenant_id, run_tenant_id)
       when is_binary(request_tenant_id) and request_tenant_id != "" and is_binary(run_tenant_id) and
              run_tenant_id != "" do
    request_tenant_id == run_tenant_id
  end

  defp tenant_allowed?(_request_tenant_id, _run_tenant_id), do: true
end
