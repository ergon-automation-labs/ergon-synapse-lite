defmodule BotArmySynapse.Orchestrator do
  @moduledoc """
  Main orchestration logic for the Synapse service.

  Handles `llm.context.analyze` requests:
  - Extracts context sources from payload
  - Queries each bot via NATS request/reply
  - Builds formatted context object
  - Calls LLM proxy for completion
  - Routes response back to requester
  """

  require Logger

  alias BotArmyRuntime.NATS.Reply
  @llm_request_types ~w(chat analysis sentiment explain)
  @llm_model_preferences ~w(auto fast powerful cheap)
  @daily_override_decisions ~w(accept snooze defer replace reorder dismiss)

  alias BotArmySynapse.Context.{State, Time}

  @doc """
  Handle request/reply Daily Command recommendation lookup.
  """
  def handle_daily_command_recommendation_current(message) do
    payload = message["payload"] || %{}
    reply_to = message["reply_to"]
    request_ctx = extract_request_context(message, payload)
    tenant_id = normalize_tenant_id(request_ctx.tenant_id)
    user_id = request_ctx.user_id

    if is_nil(user_id) or user_id == "" do
      Logger.warning("Daily command request dropped: missing user_id")

      if reply_to do
        reply_nats_json(reply_to, %{"ok" => false, "error" => "missing_user_id"})
      end

      {:error, :missing_user_id}
    else
      context_data = safe_daily_context()
      override_state = nil

      recommendation =
        build_daily_recommendation_for_scope(context_data, override_state, tenant_id, user_id)

      event = %{
        "event_id" => UUID.uuid4(),
        "event" => "daily.command.recommendation",
        "schema_version" => "1.0",
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "source" => "bot_army_synapse",
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "payload" => recommendation
      }

      status_event = %{
        "event_id" => UUID.uuid4(),
        "event" => "daily.command.status",
        "schema_version" => "1.0",
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "source" => "bot_army_synapse",
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "payload" => build_daily_command_status_payload(recommendation)
      }

      _ = publish_nats("daily.command.recommendation", event)
      _ = publish_nats("events.daily.command.recommendation", event)
      _ = publish_nats("daily.command.status", status_event)
      _ = publish_nats("events.daily.command.status", status_event)

      if reply_to do
        reply_nats_json(reply_to, Reply.ok(%{"recommendation" => recommendation}))
      end

      {:ok, recommendation}
    end
  end

  @doc """
  Consume Daily Command override events and update recommendation state.
  """
  def handle_daily_command_override(message) do
    payload = message["payload"] || %{}

    tenant_id =
      Map.get(message, "tenant_id") ||
        Map.get(payload, "tenant_id")

    user_id =
      Map.get(message, "user_id") ||
        Map.get(payload, "user_id")

    cond do
      not (is_binary(tenant_id) and tenant_id != "" and is_binary(user_id) and user_id != "") ->
        Logger.warning("Daily command override dropped: missing tenant_id/user_id")
        {:error, :missing_scope}

      not valid_daily_override_payload?(payload) ->
        Logger.warning("Daily command override dropped: invalid decision payload")
        {:error, :invalid_override_payload}

      true ->
        normalized_payload = normalize_daily_override_payload(payload)

        enriched_payload =
          normalized_payload
          |> Map.put_new("override_event_id", Map.get(message, "event_id") || UUID.uuid4())
          |> Map.put_new("tenant_id", tenant_id)
          |> Map.put_new("user_id", user_id)

        # Daily command overrides not available in lite mode

        # Record outcome: daily recommendation acceptance
        try do
          decision = Map.get(normalized_payload, "decision", "unknown")
          override_id = Map.get(enriched_payload, "override_event_id")

          BotArmyLearning.OutcomeTracker.record(
            override_id,
            "synapse.daily_recommendation",
            "user_feedback",
            decision,
            decision == "accept"
          )
        rescue
          _ -> :ok
        end

        :ok
    end
  end

  @doc """
  Start a daily review interaction by returning the period-specific question set and schema.
  """
  def handle_daily_review_start(message) do
    payload = message["payload"] || %{}
    reply_to = message["reply_to"]
    request_ctx = extract_request_context(message, payload)
    tenant_id = normalize_tenant_id(request_ctx.tenant_id)
    user_id = request_ctx.user_id
    period = normalize_review_period(Map.get(payload, "period", "morning"))

    if is_nil(user_id) or user_id == "" do
      Logger.warning("Daily review start dropped: missing user_id")

      if reply_to do
        reply_nats_json(reply_to, %{"ok" => false, "error" => "missing_user_id"})
      end

      {:error, :missing_user_id}
    else
      questions =
        case period do
          "evening" -> evening_review_question_set()
          _ -> morning_review_question_set()
        end

      review = %{
        "review_id" => UUID.uuid4(),
        "period" => period,
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "questions" => questions,
        "response_schema" => daily_review_response_schema(period),
        "generated_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }

      if reply_to do
        reply_nats_json(reply_to, Reply.ok(%{"review" => review}))
      end

      {:ok, review}
    end
  end

  @doc """
  Submit completed daily review answers and generate summary + next actions.
  """
  def handle_daily_review_submit(message) do
    payload = message["payload"] || %{}
    reply_to = message["reply_to"]
    request_ctx = extract_request_context(message, payload)
    tenant_id = normalize_tenant_id(request_ctx.tenant_id)
    user_id = request_ctx.user_id
    period = normalize_review_period(Map.get(payload, "period", "morning"))
    answers = normalize_review_answers(Map.get(payload, "answers", []))
    review_id = Map.get(payload, "review_id") || UUID.uuid4()

    cond do
      is_nil(user_id) or user_id == "" ->
        Logger.warning("Daily review submit dropped: missing user_id")

        if reply_to do
          reply_nats_json(reply_to, %{"ok" => false, "error" => "missing_user_id"})
        end

        {:error, :missing_user_id}

      answers == [] ->
        if reply_to do
          reply_nats_json(reply_to, %{"ok" => false, "error" => "missing_answers"})
        end

        {:error, :missing_answers}

      true ->
        result = %{
          "review_id" => review_id,
          "period" => period,
          "tenant_id" => tenant_id,
          "user_id" => user_id,
          "answers" => answers,
          "summary" => build_review_summary(period, answers),
          "next_actions" => build_review_next_actions(period, answers),
          "generated_at" => DateTime.utc_now() |> DateTime.to_iso8601()
        }

        event = %{
          "event_id" => UUID.uuid4(),
          "event" => "daily.review.completed",
          "schema_version" => "1.0",
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
          "source" => "bot_army_synapse",
          "tenant_id" => tenant_id,
          "user_id" => user_id,
          "payload" => result
        }

        _ = publish_nats("daily.review.completed", event)
        _ = publish_nats("events.daily.review.completed", event)

        if reply_to do
          reply_nats_json(reply_to, Reply.ok(%{"review" => result}))
        end

        {:ok, result}
    end
  end

  @doc """
  Handle LLM context analysis request.

  Expected payload structure:
  ```
  {
    "question": "What should I focus on this week?",
    "context_sources": ["gtd", "calendar", "context", "time"],
    "user_id": "abc123",
    "tenant_id": "default"
  }
  ```

  Context sources can be configured via:
  - Environment: `BOT_ARMY_SYNAPSE_CONTEXT_SOURCES` (comma-separated)
  - Application config: `config :bot_army_synapse, context_sources: ["gtd", "calendar"]`

  If not specified, defaults to `default_context_sources/0` from config (includes optional `internal_docs`).
  Salt pillar overrides: `synapse:context_sources`

  Response publishes to reply_to subject with:
  ```
  {
    "event": "llm.context.analyzed",
    "question": "What should I focus on this week?",
    "context_gathered": {...},
    "completion": "Based on your tasks..."
  }
  ```

  Also publishes to:
  - `events.synapse.context.gathered` - for observability
  - `events.synapse.session.updated` - if conversation session exists
  """
  def handle_analyze(message) do
    payload = message["payload"]
    reply_to = message["reply_to"]
    request_ctx = extract_request_context(message, payload)
    run_id = request_ctx.run_id

    question = Map.get(payload, "question", "")
    requested_sources = Map.get(payload, "context_sources", default_context_sources())
    context_sources = resolve_context_sources(requested_sources, question)
    user_id = request_ctx.user_id
    tenant_id = request_ctx.tenant_id
    session_id = Map.get(payload, "session_id") || user_id || "default"

    Logger.info(
      "Analyzing context for question from user: #{inspect(user_id)} session: #{session_id}"
    )

    track_run_start(run_id, "llm.context.analyze", %{
      "tenant_id" => tenant_id,
      "user_id" => user_id,
      "session_id" => session_id,
      "question" => question,
      "reply_to" => reply_to
    })

    publish_synapse_progress(%{
      "run_id" => run_id,
      "step" => "preflight_started",
      "tenant_id" => tenant_id,
      "user_id" => user_id
    })

    preflight = dependency_preflight()

    publish_synapse_progress(%{
      "run_id" => run_id,
      "step" => "preflight_completed",
      "tenant_id" => tenant_id,
      "user_id" => user_id,
      "preflight" => preflight
    })

    if preflight["ready"] != true do
      error_msg = "dependency_preflight_failed: " <> Enum.join(preflight["errors"] || [], ", ")

      publish_synapse_progress(%{
        "run_id" => run_id,
        "step" => "failed",
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "error" => error_msg
      })

      BotArmySynapse.RunStore.mark_status(run_id, "failed", %{"error" => error_msg})

      if reply_to do
        send_error_reply(reply_to, error_msg)
      end

      {:error, :dependency_preflight_failed}
    else
      # Gather context from each source
      publish_synapse_progress(%{
        "run_id" => run_id,
        "step" => "context_lookup_started",
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "context_sources" => context_sources
      })

      context_data = gather_context(context_sources, question)
      context_summary = format_context_summary(context_data)

      publish_synapse_progress(%{
        "run_id" => run_id,
        "step" => "context_lookup_completed",
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "context_summary" => context_summary
      })

      # Get conversation history
      conversation_history = BotArmySynapse.ConversationStore.get_history(session_id)

      # Build the formatted prompt with context + history
      prompt = build_prompt(question, context_data, conversation_history)

      # Call LLM proxy with the enriched context
      call_llm(
        prompt,
        message,
        context_data,
        reply_to,
        tenant_id,
        user_id,
        session_id,
        question,
        run_id
      )
    end
  end

  @doc false
  def dependency_preflight do
    case GenServer.whereis(BotArmyRuntime.NATS.Connection) do
      nil ->
        %{
          "ready" => false,
          "nats_connection" => "unavailable",
          "errors" => ["nats_connection_process_missing"]
        }

      _pid ->
        case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 2_000) do
          {:ok, _conn} ->
            %{
              "ready" => true,
              "nats_connection" => "ok",
              "errors" => []
            }

          {:error, reason} ->
            %{
              "ready" => false,
              "nats_connection" => "unavailable",
              "errors" => ["nats_get_connection_failed: #{inspect(reason)}"]
            }
        end
    end
  rescue
    e ->
      %{
        "ready" => false,
        "nats_connection" => "unavailable",
        "errors" => ["nats_preflight_exception: #{inspect(e)}"]
      }
  end

  @doc false
  def gather_context(sources, question \\ "") do
    registry = BotArmySynapse.ContextRegistry.sources()

    tasks =
      Enum.map(sources, fn source ->
        Task.async(fn ->
          case source do
            "context" ->
              {:context_state, BotArmySynapse.Context.State.get_current()}

            "time" ->
              {:time, BotArmySynapse.Context.Time.get_context()}

            "history" ->
              {:history, BotArmySynapse.EventHistory.get_recent()}

            name when is_map_key(registry, name) ->
              mod = Map.get(registry, name)
              {String.to_atom(name), mod.get_context()}

            _ ->
              nil
          end
        end)
      end)

    try do
      tasks
      |> Task.await_many(context_lookup_timeout_ms())
      |> Enum.reject(&is_nil/1)
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)
      |> Map.new()
    catch
      :exit, _ ->
        # Some context sources timed out — await what finished, ignore the rest
        tasks
        |> Enum.flat_map(fn task ->
          try do
            [Task.await(task, 0)]
          catch
            :exit, _ -> []
          end
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.reject(fn {_k, v} -> is_nil(v) end)
        |> Map.new()
    end
  end

  defp context_lookup_timeout_ms do
    Application.get_env(:bot_army_synapse, :context_lookup_timeout_ms, 4_000)
  end

  defp build_prompt(question, context_data, conversation_history) do
    context_parts =
      context_data
      |> Enum.map(fn
        {:gtd, data} when is_list(data) ->
          "## GTD Tasks\n" <>
            Enum.map_join(data, "\n", fn t ->
              "  - #{t["title"]} (#{t["context"]}) [#{t["priority"]}] #{t["status"]}"
            end)

        {:gtd, _} ->
          "## GTD Tasks\nNo tasks found"

        {:calendar, data} when is_list(data) ->
          "## Calendar Events\n" <>
            Enum.map_join(data, "\n", fn e ->
              "  - #{e["summary"]} at #{e["start_time"]}"
            end)

        {:calendar, _} ->
          "## Calendar Events\nNo upcoming events"

        {:context_state, data} ->
          "## Current Context\n" <>
            "Mode: #{data["mode"]}\n" <>
            "Focus: #{Map.get(data, "focus", "N/A")}"

        {:time, data} ->
          "## Time Context\n" <>
            "Now: #{data["datetime"]}\n" <>
            "Day: #{data["day_of_week"]}\n" <>
            "Hour: #{data["hour"]}\n" <>
            "Timezone: #{data["timezone"]}"

        {:fitness, data} when is_map(data) and map_size(data) > 0 ->
          "## Fitness\n#{inspect(data)}"

        {:fitness, _} ->
          nil

        {:chore, data} when is_map(data) and map_size(data) > 0 ->
          "## Chores\n#{inspect(data)}"

        {:chore, _} ->
          nil

        {:job, data} when is_map(data) and map_size(data) > 0 ->
          "## Job Pipeline\n#{inspect(data)}"

        {:job, _} ->
          nil

        {:terrain, data} when is_map(data) and map_size(data) > 0 ->
          "## Terrain (Learning)\n#{inspect(data)}"

        {:terrain, _} ->
          nil

        {:sre, data} when is_map(data) and map_size(data) > 0 ->
          "## SRE Alerts\n#{inspect(data)}"

        {:sre, _} ->
          nil

        {:advocacy, data} when is_map(data) and map_size(data) > 0 ->
          "## Advocacy\n#{inspect(data)}"

        {:advocacy, _} ->
          nil

        {:history, events} when is_list(events) and events != [] ->
          "## Recent Activity\n" <>
            Enum.map_join(Enum.take(events, 20), "\n", fn e ->
              ts = e.timestamp |> DateTime.to_time() |> Kernel.to_string()
              "  - [#{ts}] #{e.event_type}: #{e.summary}"
            end)

        {:history, _} ->
          nil

        {:fleet, data} when is_map(data) and map_size(data) > 0 ->
          format_fleet_for_prompt(data)

        {:fleet, _} ->
          nil

        {:goals, data} when is_list(data) and data != [] ->
          Logger.debug(
            "[build_prompt] Goals context: #{length(data)} goals, first goal keys: #{inspect(Map.keys(List.first(data)))}"
          )

          "## Active Goals\n" <>
            Enum.map_join(data, "\n", fn g ->
              progress = Map.get(g, :progress_summary, "")
              decisions = Map.get(g, :decision_count, 0)
              progress_text = if progress != "", do: " — #{progress}", else: ""

              "  - [#{g["area"] || "general"}] #{g["name"]} (#{g["status"]}) #{decisions} decisions#{progress_text}"
            end)

        {:goals, _} ->
          nil

        {:internal_docs, data} when is_map(data) ->
          case Map.get(data, "results") do
            results when is_list(results) and results != [] ->
              q = Map.get(data, "query", "")
              fb = if(f = Map.get(data, "fallback"), do: " (retrieval: #{f})", else: "")

              "## Internal documentation (#{String.slice(q, 0, 200)})#{fb}\n" <>
                Enum.map_join(results, "\n\n", &format_internal_docs_hit/1)

            _ ->
              nil
          end

        {:internal_docs, _} ->
          nil
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.join("\n\n")

    history_section =
      case conversation_history do
        [] ->
          ""

        _ ->
          pairs =
            Enum.map_join(conversation_history, "\n", fn exchange ->
              "  Q: #{exchange.question}\n  A: #{String.slice(exchange.answer, 0, 200)}"
            end)

          "\n\n## Conversation History\n#{pairs}"
      end

    """
    You are an AI assistant helping a user plan and prioritize their work.

    Question: #{question}

    #{context_parts}#{history_section}

    Please provide a thoughtful response based on the context above.
    Focus on actionable advice and prioritization.
    """
  end

  defp call_llm(
         prompt,
         original_message,
         context_data,
         reply_to,
         tenant_id,
         user_id,
         session_id,
         question,
         run_id
       ) do
    payload = %{
      "text" => prompt,
      "prompt_id" => UUID.uuid4(),
      "source" => "bot_army_synapse",
      "source_metadata" => %{
        "source_domain" => "synapse_context_analysis",
        "original_event_id" => original_message["event_id"],
        "session_id" => session_id
      },
      "context_data" => context_data,
      "tenant_id" => tenant_id,
      "user_id" => user_id
    }

    # Publish to LLM prompt submit
    event_data = %{
      "event" => "llm.prompt.submit",
      "event_id" => UUID.uuid4(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "source_node" => node() |> Atom.to_string(),
      "triggered_by" => "synapse_context_analyze",
      "schema_version" => "1.0",
      "payload" => payload
    }

    # Reply immediately with context, then call LLM asynchronously
    if reply_to do
      case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000) do
        {:ok, conn} ->
          reply_json =
            Jason.encode!(%{
              "status" => "success",
              "context" => context_data,
              "question" => question,
              "session_id" => session_id
            })

          Gnat.pub(conn, reply_to, reply_json)
          Logger.info("Context analysis replied with gathered context")

        {:error, reason} ->
          Logger.error("Failed to reply to context.analyze: #{inspect(reason)}")
      end
    end

    # Call LLM asynchronously without blocking
    Task.start(fn ->
      call_llm_async(event_data, context_data, session_id, question, run_id, tenant_id, user_id)
    end)
  end

  defp call_llm_async(event_data, context_data, session_id, question, run_id, tenant_id, user_id) do
    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000) do
      {:ok, conn} ->
        json = Jason.encode!(event_data)

        publish_synapse_progress(%{
          "run_id" => run_id,
          "step" => "llm_request_started",
          "tenant_id" => tenant_id,
          "user_id" => user_id
        })

        # Publish LLM request asynchronously (fire and forget)
        case Gnat.pub(conn, "llm.prompt.submit", json) do
          :ok ->
            Logger.debug("LLM prompt published asynchronously")

            publish_synapse_progress(%{
              "run_id" => run_id,
              "step" => "llm_request_dispatched",
              "tenant_id" => tenant_id,
              "user_id" => user_id
            })

            publish_context_gathered(event_data, context_data)
            # Record placeholder response
            BotArmySynapse.ConversationStore.record_exchange(
              session_id,
              question,
              "(LLM processing...)"
            )

            publish_synapse_progress(%{
              "run_id" => run_id,
              "step" => "completed",
              "tenant_id" => tenant_id,
              "user_id" => user_id
            })

            BotArmySynapse.RunStore.mark_status(run_id, "completed")

          {:error, reason} ->
            Logger.warning("Failed to publish LLM prompt: #{inspect(reason)}")

            publish_synapse_progress(%{
              "run_id" => run_id,
              "step" => "failed",
              "tenant_id" => tenant_id,
              "user_id" => user_id,
              "error" => inspect(reason)
            })

            BotArmySynapse.RunStore.mark_status(run_id, "failed", %{"error" => inspect(reason)})
        end

      {:error, reason} ->
        Logger.warning("No NATS connection for async LLM call: #{inspect(reason)}")

        publish_synapse_progress(%{
          "run_id" => run_id,
          "step" => "failed",
          "tenant_id" => tenant_id,
          "user_id" => user_id,
          "error" => inspect(reason)
        })

        BotArmySynapse.RunStore.mark_status(run_id, "failed", %{"error" => inspect(reason)})
    end
  rescue
    e ->
      Logger.error("Error in async LLM call: #{inspect(e)}")

      publish_synapse_progress(%{
        "run_id" => run_id,
        "step" => "failed",
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "error" => inspect(e)
      })

      BotArmySynapse.RunStore.mark_status(run_id, "failed", %{"error" => inspect(e)})
  end

  defp publish_context_gathered(prompt_event, context_data) do
    event_data = %{
      "event" => "events.synapse.context.gathered",
      "event_id" => UUID.uuid4(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "source_node" => node() |> Atom.to_string(),
      "triggered_by" => "synapse_context_analyze",
      "schema_version" => "1.0",
      "payload" => %{
        "context_sources_used" => Map.keys(context_data),
        "prompt_event_id" => prompt_event["event_id"],
        "context_summary" => format_context_summary(context_data)
      }
    }

    case publish_nats("events.synapse.context.gathered", event_data) do
      :ok -> Logger.debug("Published context gathered event")
      {:error, reason} -> Logger.debug("Failed to publish context gathered: #{inspect(reason)}")
    end
  end

  defp format_context_summary(context_data) do
    context_data
    |> Enum.map(fn
      {:gtd, data} when is_list(data) ->
        "GTD: #{length(data)} tasks"

      {:gtd, _} ->
        "GTD: unavailable"

      {:calendar, data} when is_list(data) ->
        "Calendar: #{length(data)} events"

      {:calendar, _} ->
        "Calendar: unavailable"

      {:context_state, data} ->
        "Context: mode=#{Map.get(data, "mode", "unknown")}"

      {:time, _} ->
        "Time: current context available"

      {:fitness, data} when is_map(data) ->
        "Fitness: available"

      {:chore, data} when is_map(data) ->
        "Chore: available"

      {:job, data} when is_map(data) ->
        "Job: #{Map.get(data, "listings", "?")} listings"

      {:terrain, data} when is_map(data) ->
        "Terrain: available"

      {:sre, data} when is_map(data) ->
        "SRE: available"

      {:advocacy, data} when is_map(data) ->
        "Advocacy: available"

      {:history, events} when is_list(events) ->
        "History: #{length(events)} events"

      {:fleet, data} when is_map(data) ->
        "Fleet: #{Map.get(data, "online_count", 0)} up, #{Map.get(data, "stale_count", 0)} stale, #{Map.get(data, "down_count", 0)} down"

      _ ->
        nil
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.join(", ")
  end

  defp quick_greeting_reply(text) when is_binary(text) do
    trimmed = String.trim(text)

    normalized =
      trimmed
      |> String.downcase()
      |> String.replace(~r/[[:punct:]]+/, "")
      |> String.replace(~r/\s+/, " ")
      |> String.trim()

    social_phrase? =
      normalized in [
        "hi",
        "hello",
        "hey",
        "yo",
        "sup",
        "hiya",
        "hola",
        "hi there",
        "hello there",
        "hey there",
        "thanks",
        "thank you",
        "thx",
        "ty",
        "good morning",
        "good afternoon",
        "good evening"
      ]

    emoji_only? =
      trimmed != "" and
        Regex.match?(
          ~r/^[\x{1F300}-\x{1FAFF}\x{2600}-\x{27BF}\x{1F1E6}-\x{1F1FF}\x{200D}\x{FE0F}\s]+$/u,
          trimmed
        )

    if social_phrase? or emoji_only? do
      {:reply, "Hey! What should we work on?"}
    else
      :continue
    end
  end

  defp personalize_quick_reply(base_text, author_name) when is_binary(author_name) do
    cleaned = String.trim(author_name)

    if cleaned == "" do
      base_text
    else
      "Hey #{cleaned} - what should we work on?"
    end
  end

  defp personalize_quick_reply(base_text, _), do: base_text

  defp send_error_reply(reply_to, error_msg) do
    response = %{
      "event" => "llm.error",
      "event_id" => UUID.uuid4(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "source_node" => node() |> Atom.to_string(),
      "schema_version" => "1.0",
      "payload" => %{
        "error" => "Context analysis failed",
        "reason" => error_msg
      }
    }

    case Jason.encode(response) do
      {:ok, body} ->
        with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
          Gnat.pub(conn, reply_to, body)
        end

      {:error, reason} ->
        Logger.error("Failed to encode error reply: #{inspect(reason)}")
    end
  end

  defp publish_nats(subject, event_data) do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000),
         {:ok, body} <- Jason.encode(event_data) do
      Gnat.pub(conn, subject, body)
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Default context sources (configurable via Salt/pillar)
  @doc false
  def default_context_sources do
    case Application.get_env(:bot_army_synapse, :context_sources) do
      nil ->
        case System.get_env("BOT_ARMY_SYNAPSE_CONTEXT_SOURCES") do
          nil -> ["gtd", "calendar", "context", "fleet_health"]
          env_str -> String.split(env_str, ",") |> Enum.map(&String.trim/1)
        end

      config_list when is_list(config_list) ->
        config_list
    end
  end

  @doc false
  def resolve_context_sources(sources, question) when is_list(sources) do
    inferred_sources = infer_context_sources(question)
    sources = Enum.uniq(sources ++ inferred_sources)
    maybe_add_internal_docs(sources, question)
  end

  def resolve_context_sources(_sources, question) do
    infer_context_sources(question)
    |> maybe_add_internal_docs(question)
  end

  @doc false
  def infer_context_sources(question) when is_binary(question) do
    q = String.downcase(question)

    []
    |> maybe_add_daily_brief_sources(q)
    |> maybe_add_source("gtd", q, ["task", "tasks", "todo", "to-do", "next action", "inbox"])
    |> maybe_add_source("goals", q, ["goal", "goals", "project", "projects", "priority"])
    |> maybe_add_source("fleet", q, ["fleet", "bot", "bots", "status", "online", "offline"])
    |> maybe_add_source("sre", q, ["runbook", "runbooks", "incident", "alert", "sre"])
  end

  def infer_context_sources(_), do: []

  @doc false
  def morning_review_question_set do
    [
      %{
        "id" => "energy_check",
        "prompt" => "How is your energy right now (low, medium, high)?",
        "intent" => "calibrate_planning_depth"
      },
      %{
        "id" => "top_outcome",
        "prompt" => "What is the one outcome that would make today a win?",
        "intent" => "identify_primary_goal"
      },
      %{
        "id" => "constraints",
        "prompt" =>
          "What hard constraints do you have today (meetings, deadlines, personal limits)?",
        "intent" => "bound_commitments"
      },
      %{
        "id" => "risks",
        "prompt" => "What could derail today, and what is your mitigation?",
        "intent" => "precommit_risk_controls"
      },
      %{
        "id" => "first_action",
        "prompt" => "What is the first concrete action you will take in the next 15 minutes?",
        "intent" => "force_immediate_start"
      }
    ]
  end

  @doc false
  def evening_review_question_set do
    [
      %{
        "id" => "wins",
        "prompt" => "What did you complete or move forward today?",
        "intent" => "capture_progress"
      },
      %{
        "id" => "misses",
        "prompt" => "What did not get done, and why?",
        "intent" => "surface_failure_modes"
      },
      %{
        "id" => "carry_forward",
        "prompt" => "What must be carried into tomorrow as top priority?",
        "intent" => "preserve_continuity"
      },
      %{
        "id" => "system_adjustment",
        "prompt" => "What one adjustment to your system would improve tomorrow?",
        "intent" => "continuous_improvement"
      },
      %{
        "id" => "shutdown",
        "prompt" => "Are you ready for shutdown (yes/no)? If no, what is missing?",
        "intent" => "close_day_cleanly"
      }
    ]
  end

  @doc false
  def daily_review_response_schema(period) when period in ["morning", "evening"] do
    %{
      "period" => period,
      "required_fields" => [
        "review_id",
        "tenant_id",
        "user_id",
        "answers",
        "summary",
        "next_actions",
        "generated_at"
      ],
      "answer_fields" => ["question_id", "response", "confidence"],
      "next_action_fields" => ["title", "source_question_id", "priority", "due_hint"]
    }
  end

  defp normalize_review_answers(answers) when is_list(answers) do
    answers
    |> Enum.map(fn
      %{"question_id" => qid, "response" => response} = answer
      when is_binary(qid) and is_binary(response) ->
        %{
          "question_id" => String.trim(qid),
          "response" => String.trim(response),
          "confidence" => normalize_answer_confidence(Map.get(answer, "confidence"))
        }

      _ ->
        nil
    end)
    |> Enum.reject(fn
      %{"question_id" => "", "response" => _} -> true
      %{"question_id" => _, "response" => ""} -> true
      nil -> true
      _ -> false
    end)
  end

  defp normalize_review_answers(_), do: []

  defp normalize_answer_confidence(confidence) when is_number(confidence) do
    confidence
    |> max(0.0)
    |> min(1.0)
  end

  defp normalize_answer_confidence(_), do: 0.7

  defp build_review_summary(period, answers) do
    focus =
      answers
      |> Enum.take(2)
      |> Enum.map(fn a -> a["response"] end)
      |> Enum.join(" | ")

    case period do
      "evening" -> "Evening reflection complete. Key themes: #{focus}"
      _ -> "Morning review complete. Primary focus: #{focus}"
    end
  end

  defp build_review_next_actions(period, answers) do
    answers
    |> Enum.take(3)
    |> Enum.with_index(1)
    |> Enum.map(fn {answer, idx} ->
      %{
        "title" => next_action_title(period, idx, answer["response"]),
        "source_question_id" => answer["question_id"],
        "priority" => if(idx == 1, do: "high", else: "medium"),
        "due_hint" => if(period == "morning", do: "today", else: "tomorrow_morning")
      }
    end)
  end

  defp next_action_title(period, idx, response) do
    prefix = if(period == "morning", do: "Morning", else: "Carry-forward")
    "#{prefix} action #{idx}: #{String.slice(response, 0, 120)}"
  end

  defp normalize_review_period(period) when is_binary(period) do
    value = period |> String.trim() |> String.downcase()
    if value in ["morning", "evening"], do: value, else: "morning"
  end

  defp normalize_review_period(_), do: "morning"

  defp maybe_add_daily_brief_sources(sources, q) when is_binary(q) do
    if daily_brief_intent?(q) do
      Enum.uniq(sources ++ ["gtd", "calendar", "context", "fitness", "chore", "goals"])
    else
      sources
    end
  end

  defp daily_brief_intent?(q) when is_binary(q) do
    String.contains?(q, "daily brief") ||
      String.contains?(q, "daily review") ||
      String.contains?(q, "day plan") ||
      String.contains?(q, "plan my day") ||
      String.contains?(q, "today's plan") ||
      String.contains?(q, "todays plan")
  end

  defp maybe_add_source(sources, source, text, keywords) do
    if Enum.any?(keywords, &String.contains?(text, &1)) do
      [source | sources]
    else
      sources
    end
  end

  defp maybe_add_internal_docs(sources, question) when is_list(sources) do
    enabled? = Application.get_env(:bot_army_synapse, :include_internal_docs_in_chat, true)
    question_present? = is_binary(question) and String.trim(question) != ""

    if enabled? and question_present? and not Enum.member?(sources, "internal_docs") do
      sources ++ ["internal_docs"]
    else
      sources
    end
  end

  @doc """
  Handle Discord chat message from surface_discord.

  These arrive as plain JSON (not the envelope format) from the Discord surface.
  Gathers context, builds a prompt, and calls the LLM. Replies directly
  via NATS to the reply_to subject so surface_discord can relay to Discord.
  """
  def handle_discord_message(message, subject, reply_to) do
    text = Map.get(message, "text", "")
    author_name = Map.get(message, "author_name", "unknown")
    channel_id = Map.get(message, "channel_id")
    command = extract_command(subject)
    user_id = Map.get(message, "user_id")
    tenant_id = Map.get(message, "tenant_id", BotArmyRuntime.Tenant.default_tenant_id())

    session_id =
      Map.get(message, "session_id") || Map.get(message, "channel_id") || "discord_default"

    Logger.info(
      "Discord message from #{author_name} on #{subject}: command=#{inspect(command)} text=#{String.slice(text, 0, 80)}"
    )

    # Handle built-in commands
    case command do
      "fleet" ->
        handle_fleet_command(reply_to)

      "status" ->
        handle_fleet_command(reply_to)

      "goals" ->
        Logger.info("[handle_discord_message] Calling handle_goals_command")
        handle_goals_command(reply_to, tenant_id, text)

      "projects" ->
        Logger.info("[handle_discord_message] Calling handle_projects_command")
        handle_projects_command(reply_to, tenant_id, text)

      "tasks" ->
        Logger.info("[handle_discord_message] Calling handle_tasks_command")
        handle_tasks_command(reply_to, tenant_id, text)

      "create" ->
        handle_create_command(reply_to, tenant_id, user_id, text)

      "list" ->
        handle_list_command(reply_to, tenant_id, text)

      "complete" ->
        handle_complete_command(reply_to, tenant_id, user_id, text)

      "pi-go" ->
        handle_pi_go_command(reply_to, tenant_id, user_id, channel_id, text)

      "pi_go" ->
        handle_pi_go_command(reply_to, tenant_id, user_id, channel_id, text)

      "pigo" ->
        handle_pi_go_command(reply_to, tenant_id, user_id, channel_id, text)

      _ ->
        _ =
          Logger.debug(
            "[Orchestrator] Explicit ask not dispatched in lite mode: #{String.slice(text, 0, 50)}"
          )

        # Check if this is a skill invocation (e.g., "summarize: text here")
        case parse_skill_invocation(text) do
          {:skill, skill_slug, payload_text} ->
            Logger.info("Discord message is a skill invocation: #{skill_slug}")

            # Route to skills bot asynchronously
            spawn(fn ->
              invoke_skill(
                skill_slug,
                payload_text,
                user_id,
                tenant_id,
                reply_to,
                session_id,
                text,
                channel_id
              )
            end)

          :no_skill ->
            case quick_greeting_reply(text) do
              {:reply, quick_reply} ->
                if reply_to,
                  do: reply_discord(reply_to, personalize_quick_reply(quick_reply, author_name))

              :continue ->
                case maybe_handle_skills_meta_request(text, tenant_id, user_id, reply_to) do
                  :handled ->
                    :ok

                  :continue ->
                    case maybe_handle_bridge_listing_request(
                           text,
                           tenant_id,
                           user_id,
                           reply_to,
                           session_id
                         ) do
                      :handled ->
                        :ok

                      :continue ->
                        conversation_history =
                          BotArmySynapse.ConversationStore.get_history(session_id)

                        case maybe_handle_gtd_creation_request(
                               text,
                               conversation_history,
                               tenant_id,
                               user_id,
                               reply_to,
                               channel_id
                             ) do
                          :handled ->
                            :ok

                          :continue ->
                            # GeneralPurposeDiscord not available in lite mode
                            :not_handled ->

                              :continue ->
                                run_id = UUID.uuid4()

                                progress_base = %{
                                  "run_id" => run_id,
                                  "channel_id" => channel_id,
                                  "tenant_id" => tenant_id,
                                  "user_id" => user_id
                                }

                                track_run_start(run_id, "discord.chat", %{
                                  "tenant_id" => tenant_id,
                                  "user_id" => user_id,
                                  "session_id" => session_id,
                                  "channel_id" => channel_id,
                                  "question" => text
                                })

                                publish_synapse_progress(
                                  Map.merge(progress_base, %{"step" => "preflight_started"})
                                )

                                # Process LLM synchronously so Gnat.request gets the actual response
                                context_sources =
                                  resolve_context_sources(
                                    default_context_sources() ++ ["time"],
                                    text
                                  )

                                publish_synapse_progress(
                                  Map.merge(progress_base, %{
                                    "step" => "context_lookup_started",
                                    "context_sources" => context_sources
                                  })
                                )

                                context_data = gather_context(context_sources, text)
                                context_summary = format_context_summary(context_data)

                                publish_synapse_progress(
                                  Map.merge(progress_base, %{
                                    "step" => "context_lookup_completed",
                                    "context_summary" => context_summary
                                  })
                                )

                                prompt =
                                  build_discord_prompt(
                                    text,
                                    author_name,
                                    command,
                                    context_data,
                                    conversation_history
                                  )

                                publish_synapse_progress(
                                  Map.merge(progress_base, %{"step" => "llm_request_started"})
                                )

                                case call_llm_sync(prompt,
                                       request_ctx: %{
                                         tenant_id: tenant_id,
                                         user_id: user_id,
                                         run_id: run_id
                                       }
                                     ) do
                                  {:ok, response_text} ->
                                    response_text = sanitize_discord_llm_response(response_text)

                                    publish_synapse_progress(
                                      Map.merge(progress_base, %{"step" => "finalizing_response"})
                                    )

                                    {recorded_reply, discord_reply} =
                                      case parse_pi_go_delegate_directive(response_text) do
                                        {:delegate, pi_prompt} ->
                                          params = %{
                                            "command" => "run",
                                            "prompt" => pi_prompt,
                                            "tenant_id" => tenant_id,
                                            "user_id" => user_id,
                                            "correlation_id" => UUID.uuid4(),
                                            "discord_channel_id" => channel_id
                                          }

                                          ack =
                                            case dispatch_pi_go_command(params) do
                                              %{"status" => "accepted", "data" => data} ->
                                                cid = Map.get(data, "correlation_id", "unknown")

                                                "This Discord chat path cannot read or write your Mac directly. " <>
                                                  "I sent the work to pi-go instead (correlation_id=#{cid}). " <>
                                                  "You should get a pi-go completed or failed follow-up here when it finishes."

                                              %{"status" => "error", "error" => err} ->
                                                "I tried to hand off to pi-go but dispatch failed: #{inspect(err)}"

                                              other ->
                                                "Unexpected pi-go dispatch result: #{inspect(other)}"
                                            end

                                          {ack, ack}

                                        :none ->
                                          {response_text, response_text}
                                      end

                                    BotArmySynapse.ConversationStore.record_exchange(
                                      session_id,
                                      text,
                                      recorded_reply
                                    )

                                    Logger.debug(
                                      "Discord LLM response: #{String.slice(recorded_reply, 0, 100)}"
                                    )

                                    if reply_to do
                                      reply_discord(reply_to, discord_reply)
                                    end

                                    publish_synapse_progress(
                                      Map.merge(progress_base, %{"step" => "completed"})
                                    )

                                    BotArmySynapse.RunStore.mark_status(run_id, "completed")

                                  {:error, reason} ->
                                    Logger.warning(
                                      "LLM failed for Discord message: #{inspect(reason)}"
                                    )

                                    fallback =
                                      build_discord_fallback_response(text, context_data, reason)

                                    BotArmySynapse.ConversationStore.record_exchange(
                                      session_id,
                                      text,
                                      fallback
                                    )

                                    if reply_to do
                                      reply_discord(reply_to, fallback)
                                    end

                                    publish_synapse_progress(
                                      Map.merge(progress_base, %{
                                        "step" => "failed",
                                        "error" => inspect(reason)
                                      })
                                    )

                                    BotArmySynapse.RunStore.mark_status(run_id, "failed", %{
                                      "error" => inspect(reason)
                                    })
                                end
                            end
                        end
                    end
                end
            end
        end
    end
  end

  defp maybe_handle_bridge_listing_request(text, tenant_id, user_id, reply_to, session_id) do
    if bridge_listing_intent?(text) do
      handle_grouped_gtd_listing(reply_to, tenant_id, user_id, session_id, text)

      :handled
    else
      :continue
    end
  end

  defp bridge_listing_intent?(text) when is_binary(text) do
    normalized = String.downcase(text)

    mentions_bridge? = String.contains?(normalized, "bridge")

    asks_tasks_or_projects? =
      String.contains?(normalized, "task") ||
        String.contains?(normalized, "tasks") ||
        String.contains?(normalized, "project") ||
        String.contains?(normalized, "projects")

    asks_listing? =
      String.contains?(normalized, "list") ||
        String.contains?(normalized, "what") ||
        String.contains?(normalized, "available") ||
        String.contains?(normalized, "show") ||
        String.contains?(normalized, "grouped")

    asks_tasks_or_projects? and asks_listing? and
      (mentions_bridge? or natural_task_listing_intent?(normalized))
  end

  defp bridge_listing_intent?(_), do: false

  defp natural_task_listing_intent?(normalized) when is_binary(normalized) do
    String.contains?(normalized, "task list") ||
      String.contains?(normalized, "tasks list") ||
      String.contains?(normalized, "project list") ||
      String.contains?(normalized, "projects list") ||
      String.contains?(normalized, "my tasks") ||
      String.contains?(normalized, "my projects") ||
      String.contains?(normalized, "tasks do i have") ||
      String.contains?(normalized, "projects do i have")
  end

  defp handle_grouped_gtd_listing(reply_to, tenant_id, _user_id, _session_id, _text) do
    task_params = %{"tenant_id" => tenant_id, "limit" => 200}

    project_envelope = %{
      "event" => "gtd.project.list",
      "event_id" => UUID.uuid4(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "source_node" => node() |> Atom.to_string(),
      "triggered_by" => "discord_tasks_projects_grouped",
      "schema_version" => "1.0",
      "payload" => %{"tenant_id" => tenant_id}
    }

    case request_nats("gtd.task.list", task_params, 1_200) do
      {:ok, tasks_body} ->
        case Jason.decode(tasks_body) do
          {:ok, tasks_decoded} ->
            tasks = get_in(tasks_decoded, ["data", "tasks"]) || []
            projects_result = maybe_get_projects_fast(project_envelope)
            response = build_grouped_or_tasks_only_response(tasks, projects_result)
            if reply_to, do: reply_discord(reply_to, response)

          {:error, decode_reason} ->
            Logger.warning("[grouped_gtd_listing] tasks decode failed: #{inspect(decode_reason)}")

            if reply_to,
              do:
                reply_discord(
                  reply_to,
                  "status: rejected — gtd listing failed: invalid task response"
                )
        end

      {:error, reason} ->
        Logger.warning("[grouped_gtd_listing] task request failed: #{inspect(reason)}")

        if reply_to,
          do: reply_discord(reply_to, "status: rejected — gtd listing failed: #{inspect(reason)}")

      other ->
        Logger.warning("[grouped_gtd_listing] unexpected task result: #{inspect(other)}")

        if reply_to,
          do:
            reply_discord(
              reply_to,
              "status: rejected — gtd listing failed: unexpected response shape"
            )
    end
  rescue
    e ->
      Logger.error("[grouped_gtd_listing] exception: #{inspect(e)}")

      if reply_to,
        do: reply_discord(reply_to, "status: rejected — gtd listing failed: #{inspect(e)}")
  end

  defp maybe_get_projects_fast(project_envelope) do
    task = Task.async(fn -> request_nats("gtd.project.list", project_envelope, 1_000) end)

    case Task.yield(task, 250) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, :timeout}
    end
  end

  defp build_grouped_or_tasks_only_response(tasks, projects_result) when is_list(tasks) do
    projects =
      case projects_result do
        {:ok, projects_body} ->
          case Jason.decode(projects_body) do
            {:ok, projects_decoded} -> get_in(projects_decoded, ["data", "projects"]) || []
            _ -> []
          end

        _ ->
          []
      end

    if Enum.empty?(projects) do
      format_tasks_only_response(tasks)
    else
      format_tasks_grouped_by_project(tasks, projects)
    end
  end

  defp format_tasks_only_response(tasks) when is_list(tasks) do
    if Enum.empty?(tasks) do
      "No active tasks found."
    else
      lines =
        tasks
        |> Enum.map(fn task ->
          title = Map.get(task, "title") || Map.get(task, "name") || "(untitled task)"
          status = Map.get(task, "status") || "unknown"
          priority = Map.get(task, "priority") || "normal"
          "• #{title} [#{priority}] (#{status})"
        end)
        |> Enum.join("\n")

      "Tasks:\n\n#{lines}"
    end
  end

  defp format_tasks_grouped_by_project(tasks, projects)
       when is_list(tasks) and is_list(projects) do
    if Enum.empty?(tasks) do
      "No active tasks found."
    else
      project_names =
        projects
        |> Enum.reduce(%{}, fn project, acc ->
          id = Map.get(project, "id")
          name = Map.get(project, "name")
          if is_binary(id) and is_binary(name), do: Map.put(acc, id, name), else: acc
        end)

      grouped =
        Enum.group_by(tasks, fn task ->
          project_id = Map.get(task, "project_id")
          Map.get(project_names, project_id, "Inbox / Unassigned")
        end)

      rendered_groups =
        grouped
        |> Enum.sort_by(fn {project_name, _} -> String.downcase(project_name) end)
        |> Enum.map(fn {project_name, grouped_tasks} ->
          lines =
            grouped_tasks
            |> Enum.map(fn task ->
              title = Map.get(task, "title") || Map.get(task, "name") || "(untitled task)"
              status = Map.get(task, "status") || "unknown"
              priority = Map.get(task, "priority") || "normal"
              "• #{title} [#{priority}] (#{status})"
            end)
            |> Enum.join("\n")

          "**#{project_name}**\n#{lines}"
        end)
        |> Enum.join("\n\n")

      "Tasks grouped by project:\n\n#{rendered_groups}"
    end
  end

  defp build_discord_fallback_response(question, context_data, reason) do
    base =
      context_data
      |> Enum.map(fn
        {:gtd, tasks} when is_list(tasks) ->
          "GTD has #{length(tasks)} task#{if length(tasks) == 1, do: "", else: "s"}."

        {:calendar, events} when is_list(events) ->
          "Calendar has #{length(events)} upcoming event#{if length(events) == 1, do: "", else: "s"}."

        {:context_state, state} when is_map(state) ->
          "Current mode: #{Map.get(state, "mode", "unknown")}."

        _ ->
          nil
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.join(" ")

    summary =
      if String.trim(base) == "" do
        "I gathered partial context but couldn't complete LLM reasoning right now."
      else
        "I gathered partial context while LLM was unavailable: #{base}"
      end

    trimmed_question = String.slice(String.trim(question || ""), 0, 160)

    "Temporary fallback (#{inspect(reason)}): #{summary}\nQuestion: #{trimmed_question}\nTry again in a few seconds."
  end

  defp publish_synapse_progress(payload) when is_map(payload) do
    payload = enrich_progress_payload(payload)

    BotArmyCore.NATS.publish("events.synapse.run.progress", %{
      "event_id" => UUID.uuid4(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "source_node" => node() |> Atom.to_string(),
      "triggered_by" => "discord_message",
      "schema_version" => "1.0",
      "payload" => payload
    })
  rescue
    e ->
      Logger.debug("[publish_synapse_progress] skipped due to publish error: #{inspect(e)}")
      :ok
  end

  @doc false
  def enrich_progress_payload(payload) when is_map(payload) do
    run_id = Map.get(payload, "run_id")
    step = Map.get(payload, "step")
    now_ms = System.monotonic_time(:millisecond)

    if is_binary(run_id) and run_id != "" and is_binary(step) and step != "" do
      run = BotArmySynapse.RunStore.get_run(run_id) || %{}
      {timing, next_metadata} = build_progress_timing(run, step, now_ms)
      current_status = Map.get(run, "status", "running")
      BotArmySynapse.RunStore.mark_status(run_id, current_status, %{"metadata" => next_metadata})

      payload
      |> Map.put("emitted_at", DateTime.utc_now() |> DateTime.to_iso8601())
      |> Map.put("timing", timing)
    else
      payload
    end
  rescue
    _ ->
      payload
  end

  @doc false
  def build_progress_timing(run, step, now_ms) when is_map(run) and is_binary(step) do
    metadata = Map.get(run, "metadata", %{})
    run_start_ms = Map.get(metadata, "run_start_ms", now_ms)
    last_progress_ms = Map.get(metadata, "last_progress_ms")
    run_elapsed_ms = max(now_ms - run_start_ms, 0)

    step_elapsed_ms =
      if is_integer(last_progress_ms) do
        max(now_ms - last_progress_ms, 0)
      else
        nil
      end

    timing = %{
      "run_elapsed_ms" => run_elapsed_ms,
      "step_elapsed_ms" => step_elapsed_ms
    }

    next_metadata =
      metadata
      |> Map.put("run_start_ms", run_start_ms)
      |> Map.put("last_progress_ms", now_ms)
      |> Map.put("last_step", step)

    {timing, next_metadata}
  end

  defp extract_command(subject) do
    case String.split(subject, ".") do
      ["discord", "command", command] -> command
      _ -> nil
    end
  end

  defp handle_fleet_command(reply_to) do
    fleet = Fleet.get_context() || %{}
    summary = format_fleet_command_response(fleet)

    if reply_to do
      reply_discord(reply_to, summary)
    end
  rescue
    e ->
      Logger.error("[handle_fleet_command] Exception: #{inspect(e)}")

      if reply_to do
        reply_discord(reply_to, "Unable to retrieve fleet status right now.")
      end
  end

  defp format_fleet_command_response(fleet) do
    online_count = Map.get(fleet, "online_count", 0)
    stale_count = Map.get(fleet, "stale_count", 0)
    down_count = Map.get(fleet, "down_count", 0)
    online = Map.get(fleet, "online", [])
    down = Map.get(fleet, "down", [])

    online_preview =
      online
      |> Enum.take(8)
      |> Enum.join(", ")
      |> case do
        "" -> "none"
        list -> list
      end

    down_preview =
      down
      |> Enum.take(8)
      |> Enum.join(", ")
      |> case do
        "" -> "none"
        list -> list
      end

    "Fleet status: #{online_count} online, #{stale_count} stale, #{down_count} down.\nOnline: #{online_preview}\nDown: #{down_preview}"
  end

  defp handle_pi_go_command(reply_to, tenant_id, user_id, channel_id, text) do
    params = %{
      "command" => "run",
      "prompt" => String.trim(text || ""),
      "tenant_id" => tenant_id,
      "user_id" => user_id,
      "correlation_id" => UUID.uuid4(),
      "discord_channel_id" => channel_id
    }

    case dispatch_pi_go_command(params) do
      %{"status" => "accepted", "data" => data} ->
        correlation_id = Map.get(data, "correlation_id", "unknown")
        command = Map.get(data, "command", "run")

        if reply_to do
          reply_discord(
            reply_to,
            "Sent to pi-go (command=#{command}, correlation_id=#{correlation_id})."
          )
        end

      %{"status" => "error", "error" => error} ->
        if reply_to do
          reply_discord(reply_to, "pi-go dispatch failed: #{error}")
        end

      other ->
        Logger.warning("[handle_pi_go_command] Unexpected dispatch result: #{inspect(other)}")

        if reply_to do
          reply_discord(reply_to, "pi-go dispatch returned an unexpected response.")
        end
    end
  rescue
    e ->
      Logger.error("[handle_pi_go_command] Exception: #{inspect(e)}")

      if reply_to do
        reply_discord(reply_to, "Unable to dispatch pi-go command right now.")
      end
  end

  defp handle_goals_command(reply_to, tenant_id, text) do
    goals =
      BotArmySynapse.GoalStore.list_goals(:active)
      |> filter_goals_by_tenant(tenant_id)

    Logger.info(
      "[handle_goals_command] tenant_id=#{inspect(tenant_id)} text=#{inspect(text)} goals=#{inspect(goals, limit: 3)}"
    )

    sort_by = parse_goals_sort_option(text)
    filter_label = parse_goals_filter_label(text)

    response =
      case goals do
        nil ->
          "No goals found."

        [] ->
          "No active goals."

        goal_list when is_list(goal_list) ->
          filtered =
            if filter_label do
              Enum.filter(goal_list, fn goal ->
                labels = Map.get(goal, "labels", [])
                Enum.any?(labels, &(String.downcase(&1) == String.downcase(filter_label)))
              end)
            else
              goal_list
            end

          sorted =
            case sort_by do
              :recent ->
                Enum.sort_by(filtered, fn goal ->
                  goal[:last_decision_at] || goal["last_decision_at"] || ""
                end)
                |> Enum.reverse()

              :active ->
                Enum.sort_by(filtered, fn goal ->
                  -(goal[:decision_count] || goal["decision_count"] || 0)
                end)

              _ ->
                filtered
            end

          if Enum.empty?(sorted) do
            "No goals match your criteria."
          else
            formatted =
              sorted
              |> Enum.map(fn goal ->
                name = goal["name"]
                labels_str = format_goal_labels(goal["labels"])
                decisions = goal[:decision_count] || goal["decision_count"] || 0

                last_decision =
                  format_last_decision(goal[:last_decision_at] || goal["last_decision_at"])

                "• **#{name}**#{labels_str} — #{decisions} decision#{if decisions != 1, do: "s", else: ""} #{last_decision}"
              end)
              |> Enum.join("\n")

            header =
              case {sort_by, filter_label} do
                {_, label} when label != nil -> "**Goals with ##{label}:**"
                {:recent, _} -> "**Goals by Recent Decisions:**"
                {:active, _} -> "**Goals by Momentum:**"
                _ -> "**Active Goals:**"
              end

            "#{header}\n#{formatted}"
          end

        _ ->
          "Unable to retrieve goals."
      end

    if reply_to do
      reply_discord(reply_to, response)
    end
  rescue
    e ->
      Logger.error("[handle_goals_command] Exception: #{inspect(e)}")

      if reply_to do
        reply_discord(reply_to, "Error retrieving goals: #{inspect(e)}")
      end
  end

  defp handle_projects_command(reply_to, tenant_id, text) do
    spawn(fn ->
      try do
        envelope = %{
          "event" => "gtd.project.list",
          "event_id" => UUID.uuid4(),
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
          "source" => "bot_army_synapse",
          "source_node" => node() |> Atom.to_string(),
          "triggered_by" => "discord_projects_command",
          "schema_version" => "1.0",
          "payload" => %{"tenant_id" => tenant_id}
        }

        case request_nats("gtd.project.list", envelope) do
          {:ok, body} ->
            case Jason.decode(body) do
              {:ok, decoded} ->
                projects = get_in(decoded, ["data", "projects"]) || []
                filter_label = parse_goals_filter_label(text)

                filtered =
                  case filter_label do
                    nil -> projects
                    label -> Enum.filter(projects, &(label in (Map.get(&1, "labels", []) || [])))
                  end

                active = Enum.filter(filtered, &(Map.get(&1, "status") == "active"))

                response =
                  if active == [] do
                    "No active projects."
                  else
                    formatted =
                      Enum.map(active, fn p ->
                        area = if a = p["area"], do: " [#{a}]", else: ""
                        labels_str = format_goal_labels(p["labels"])
                        "• **#{p["name"]}**#{area}#{labels_str} — active"
                      end)
                      |> Enum.join("\n")

                    "**Active Projects:**\n#{formatted}"
                  end

                reply_discord(reply_to, response)

              {:error, _} ->
                reply_discord(reply_to, "Failed to parse project data.")
            end

          {:error, reason} ->
            Logger.warning("[handle_projects_command] NATS error: #{inspect(reason)}")
            reply_discord(reply_to, "Unable to retrieve projects right now.")
        end
      rescue
        e ->
          Logger.error("[handle_projects_command] Exception: #{inspect(e)}")
          reply_discord(reply_to, "Error retrieving projects: #{inspect(e)}")
      end
    end)
  end

  defp handle_tasks_command(reply_to, tenant_id, text) do
    spawn(fn ->
      try do
        filter_label = parse_goals_filter_label(text)

        params =
          %{
            "tenant_id" => tenant_id,
            "limit" => 50
          }
          |> then(fn p ->
            if filter_label, do: Map.put(p, "labels", filter_label), else: p
          end)

        case request_nats("gtd.task.list", params) do
          {:ok, body} ->
            case Jason.decode(body) do
              {:ok, decoded} ->
                tasks = get_in(decoded, ["data", "tasks"]) || []

                response =
                  if tasks == [] do
                    "No active tasks."
                  else
                    formatted =
                      Enum.map(tasks, fn t ->
                        ctx = if c = t["context"], do: " (#{c})", else: ""
                        pri = t["priority"] || "normal"
                        status = t["status"]

                        due =
                          if d = t["due_date"],
                            do: " due: #{d}",
                            else: ""

                        "• **#{t["title"]}**#{ctx} [#{pri}] #{status}#{due}"
                      end)
                      |> Enum.join("\n")

                    total = length(tasks)
                    "**Tasks (#{total}):**\n#{formatted}"
                  end

                reply_discord(reply_to, response)

              {:error, _} ->
                reply_discord(reply_to, "Failed to parse task data.")
            end

          {:error, reason} ->
            Logger.warning("[handle_tasks_command] NATS error: #{inspect(reason)}")
            reply_discord(reply_to, "Unable to retrieve tasks right now.")
        end
      rescue
        e ->
          Logger.error("[handle_tasks_command] Exception: #{inspect(e)}")
          reply_discord(reply_to, "Error retrieving tasks: #{inspect(e)}")
      end
    end)
  end

  defp handle_create_command(reply_to, tenant_id, user_id, text) do
    parsed = parse_create_command(text)

    case parsed do
      {:project, project_name} ->
        create_project_and_tasks(project_name, [], tenant_id, user_id, reply_to)

      {:task, task_title} ->
        create_project_and_tasks(nil, [task_title], tenant_id, user_id, reply_to)

      {:project_and_tasks, project_name, tasks} ->
        create_project_and_tasks(project_name, tasks, tenant_id, user_id, reply_to)

      :invalid ->
        if reply_to do
          reply_discord(
            reply_to,
            "Usage: `!create project <name>` or `!create task <title>` or multi-line `Project: ...` + numbered task list."
          )
        end
    end
  end

  defp handle_list_command(reply_to, tenant_id, text) do
    if String.starts_with?(String.downcase(String.trim(text || "")), "recent") do
      handle_list_recent_command(reply_to, tenant_id, text)
    else
      if reply_to do
        reply_discord(reply_to, "Usage: `!list recent [tasks|projects|all] [24h|7d|30d]`")
      end
    end
  end

  defp handle_complete_command(reply_to, tenant_id, user_id, text) do
    task_id = text |> String.trim() |> String.split(~r/\s+/, parts: 2) |> List.first()

    if is_binary(task_id) and task_id != "" do
      envelope = %{
        "event" => "gtd.task.complete",
        "event_id" => UUID.uuid4(),
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "source" => "bot_army_synapse",
        "source_node" => node() |> Atom.to_string(),
        "triggered_by" => "discord_message",
        "schema_version" => "1.0",
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "payload" => %{"task_id" => task_id}
      }

      response =
        case request_nats("gtd.task.complete", envelope) do
          {:ok, _body} -> "Completed task `#{task_id}`."
          {:error, reason} -> "Failed to complete `#{task_id}`: #{inspect(reason)}"
        end

      if reply_to, do: reply_discord(reply_to, response)
    else
      if reply_to do
        reply_discord(reply_to, "Usage: `!complete <task_id>`")
      end
    end
  end

  defp handle_list_recent_command(reply_to, tenant_id, text) do
    now = DateTime.utc_now()
    window_hours = parse_recent_window_hours(text)
    cutoff = DateTime.add(now, -window_hours * 3600, :second)
    target = parse_recent_target(text)

    response =
      case target do
        :tasks ->
          list_recent_tasks(tenant_id, cutoff, now)

        :projects ->
          list_recent_projects(tenant_id, cutoff, now)

        :all ->
          [
            list_recent_projects(tenant_id, cutoff, now),
            list_recent_tasks(tenant_id, cutoff, now)
          ]
          |> Enum.join("\n\n")
      end

    if reply_to, do: reply_discord(reply_to, response)
  end

  defp parse_create_command(text) when is_binary(text) do
    trimmed = String.trim(text)

    cond do
      Regex.match?(~r/^project\s+.+$/i, trimmed) ->
        [name] = Regex.run(~r/^project\s+(.+)$/i, trimmed, capture: :all_but_first)
        {:project, String.trim(name)}

      Regex.match?(~r/^task\s+.+$/i, trimmed) ->
        [title] = Regex.run(~r/^task\s+(.+)$/i, trimmed, capture: :all_but_first)
        {:task, String.trim(title)}

      true ->
        parsed = parse_project_and_tasks_block(trimmed)

        cond do
          parsed.project_name && parsed.tasks != [] ->
            {:project_and_tasks, parsed.project_name, parsed.tasks}

          parsed.project_name ->
            {:project, parsed.project_name}

          parsed.tasks != [] ->
            {:task, List.first(parsed.tasks)}

          true ->
            :invalid
        end
    end
  end

  defp parse_create_command(_), do: :invalid

  defp parse_recent_target(text) do
    lower = String.downcase(text || "")

    cond do
      String.contains?(lower, "projects") -> :projects
      String.contains?(lower, "tasks") -> :tasks
      true -> :all
    end
  end

  defp parse_recent_window_hours(text) do
    lower = String.downcase(text || "")

    cond do
      Regex.match?(~r/\b30\s*d(?:ays?)?\b/, lower) ->
        24 * 30

      Regex.match?(~r/\b7\s*d(?:ays?)?\b/, lower) ->
        24 * 7

      Regex.match?(~r/\b24\s*h(?:ours?)?\b/, lower) ->
        24

      Regex.match?(~r/\b\d+\s*h(?:ours?)?\b/, lower) ->
        [hours] = Regex.run(~r/\b(\d+)\s*h(?:ours?)?\b/, lower, capture: :all_but_first)
        String.to_integer(hours)

      true ->
        24
    end
  end

  defp list_recent_tasks(tenant_id, cutoff, now) do
    params = %{"tenant_id" => tenant_id, "limit" => 200}

    case request_nats("gtd.task.list", params) do
      {:ok, body} ->
        tasks =
          body
          |> parse_body()
          |> get_in(["data", "tasks"])
          |> Kernel.||([])
          |> Enum.filter(&recent_enough?(&1, cutoff))

        if tasks == [] do
          "Recent tasks: none in the selected window."
        else
          header = "Recent tasks (#{length(tasks)} in last #{hours_between(cutoff, now)}h):"

          lines =
            tasks
            |> Enum.take(20)
            |> Enum.map(fn task ->
              id = Map.get(task, "id", "unknown")
              title = Map.get(task, "title", "(untitled)")
              status = Map.get(task, "status", "unknown")
              "• `#{id}` — #{title} [#{status}]"
            end)
            |> Enum.join("\n")

          "#{header}\n#{lines}"
        end

      {:error, reason} ->
        "Failed to list recent tasks: #{inspect(reason)}"
    end
  end

  defp list_recent_projects(tenant_id, cutoff, now) do
    envelope = %{
      "event" => "gtd.project.list",
      "event_id" => UUID.uuid4(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "source_node" => node() |> Atom.to_string(),
      "triggered_by" => "discord_message",
      "schema_version" => "1.0",
      "payload" => %{"tenant_id" => tenant_id}
    }

    case request_nats("gtd.project.list", envelope) do
      {:ok, body} ->
        projects =
          body
          |> parse_body()
          |> get_in(["data", "projects"])
          |> Kernel.||([])
          |> Enum.filter(&recent_enough?(&1, cutoff))

        if projects == [] do
          "Recent projects: none in the selected window."
        else
          header = "Recent projects (#{length(projects)} in last #{hours_between(cutoff, now)}h):"

          lines =
            projects
            |> Enum.take(20)
            |> Enum.map(fn project ->
              id = Map.get(project, "id", "unknown")
              name = Map.get(project, "name", "(unnamed)")
              status = Map.get(project, "status", "unknown")
              "• `#{id}` — #{name} [#{status}]"
            end)
            |> Enum.join("\n")

          "#{header}\n#{lines}"
        end

      {:error, reason} ->
        "Failed to list recent projects: #{inspect(reason)}"
    end
  end

  defp recent_enough?(item, cutoff) when is_map(item) do
    iso = Map.get(item, "updated_at") || Map.get(item, "created_at")

    with true <- is_binary(iso),
         {:ok, dt, _offset} <- parse_datetime(iso) do
      DateTime.compare(dt, cutoff) in [:gt, :eq]
    else
      _ -> false
    end
  end

  defp recent_enough?(_, _), do: false

  defp hours_between(from_dt, to_dt) do
    DateTime.diff(to_dt, from_dt, :hour)
  end

  defp parse_datetime(iso) when is_binary(iso) do
    case DateTime.from_iso8601(iso) do
      {:ok, dt, offset} ->
        {:ok, dt, offset}

      _ ->
        DateTime.from_iso8601(iso <> "Z")
    end
  end

  defp maybe_handle_gtd_creation_request(
         text,
         conversation_history,
         tenant_id,
         user_id,
         reply_to,
         channel_id
       ) do
    if gtd_creation_intent?(text) do
      skill_payload = build_gtd_create_skill_payload(text, conversation_history)

      # Route natural-language GTD creation through the safe-create skill so
      # the response is grounded in bridge-backed IDs instead of free-form text.
      spawn(fn ->
        invoke_skill(
          "synapse-gtd-create-safe",
          skill_payload,
          user_id,
          tenant_id,
          reply_to,
          "discord_gtd_create",
          text,
          channel_id
        )
      end)

      :handled
    else
      :continue
    end
  end

  defp gtd_creation_intent?(text) when is_binary(text) do
    normalized = String.downcase(text)

    has_create_verb? =
      Enum.any?(["create", "make", "add", "log", "open", "start"], fn verb ->
        String.contains?(normalized, verb)
      end)

    has_gtd_object? =
      Enum.any?(["gtd", "task", "todo", "to-do", "project"], fn object ->
        String.contains?(normalized, object)
      end)

    has_create_verb? and has_gtd_object?
  end

  defp gtd_creation_intent?(_), do: false

  defp build_gtd_create_skill_payload(text, conversation_history) do
    case extract_project_and_tasks(text, conversation_history) do
      {:ok, %{project_name: project_name, tasks: tasks}} when is_list(tasks) and tasks != [] ->
        tasks_block =
          tasks
          |> Enum.with_index(1)
          |> Enum.map(fn {task, index} -> "#{index}. #{task}" end)
          |> Enum.join("\n")

        if is_binary(project_name) and String.trim(project_name) != "" do
          "Create this in GTD and return real IDs only.\nProject: #{project_name}\n#{tasks_block}"
        else
          "Create these tasks in GTD and return real IDs only.\n#{tasks_block}"
        end

      _ ->
        text
    end
  end

  defp extract_project_and_tasks(text, conversation_history) do
    parsed_from_text = parse_project_and_tasks_block(text)

    if parsed_from_text.tasks != [] do
      {:ok, parsed_from_text}
    else
      case extract_latest_assistant_answer(conversation_history) do
        nil ->
          :error

        answer ->
          parsed_from_history = parse_project_and_tasks_block(answer)

          if parsed_from_history.tasks == [] do
            :error
          else
            {:ok, parsed_from_history}
          end
      end
    end
  end

  defp extract_latest_assistant_answer(history) when is_list(history) do
    history
    |> Enum.reverse()
    |> Enum.find_value(fn
      %{answer: answer} when is_binary(answer) and answer != "" -> answer
      %{"answer" => answer} when is_binary(answer) and answer != "" -> answer
      _ -> nil
    end)
  end

  defp extract_latest_assistant_answer(_), do: nil

  defp parse_project_and_tasks_block(text) when is_binary(text) do
    lines = String.split(text, ~r/\r?\n/)

    project_name =
      Enum.find_value(lines, fn line ->
        case Regex.run(~r/^\s*project\s*:\s*(.+)\s*$/i, line, capture: :all_but_first) do
          [name] -> String.trim(name)
          _ -> nil
        end
      end)

    tasks =
      lines
      |> Enum.flat_map(fn line ->
        case Regex.run(~r/^\s*(?:\d+[\.\)]|[-*])\s+(.+)\s*$/, line, capture: :all_but_first) do
          [task] -> [normalize_task_line(task)]
          _ -> []
        end
      end)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()

    %{project_name: project_name, tasks: tasks}
  end

  defp parse_project_and_tasks_block(_), do: %{project_name: nil, tasks: []}

  defp normalize_task_line(line) do
    line
    |> String.replace(~r/\*\*/, "")
    |> String.replace(~r/^\s*Tasks?\s*\([^)]*\)\s*:\s*/i, "")
    |> String.trim()
  end

  defp create_project_and_tasks(project_name, tasks, tenant_id, user_id, reply_to) do
    resolved_project_name =
      if is_binary(project_name) and String.trim(project_name) != "" do
        String.trim(project_name)
      else
        "Discord capture #{Date.utc_today()}"
      end

    project_payload = %{
      "name" => resolved_project_name,
      "tenant_id" => tenant_id,
      "user_id" => user_id
    }

    {project_id, project_error} =
      case request_nats("bridge.project.create", project_payload) do
        {:ok, body} ->
          parsed = parse_body(body)
          id = get_in(parsed, ["data", "project_id"]) || get_in(parsed, ["data", "project", "id"])
          {id, nil}

        {:error, reason} ->
          {nil, inspect(reason)}
      end

    {created_task_ids, task_errors} =
      Enum.reduce(tasks, {[], []}, fn title, {ids, errors} ->
        task_payload =
          %{
            "title" => title,
            "context" => "inbox",
            "priority" => "normal",
            "tenant_id" => tenant_id,
            "user_id" => user_id
          }
          |> maybe_put_project_id(project_id)

        case request_nats("bridge.task.create", task_payload) do
          {:ok, body} ->
            parsed = parse_body(body)

            task_id =
              get_in(parsed, ["data", "task_id"]) || get_in(parsed, ["data", "task", "id"])

            if is_binary(task_id) and task_id != "" do
              {[task_id | ids], errors}
            else
              {ids, ["task create returned no task_id for `#{title}`" | errors]}
            end

          {:error, reason} ->
            {ids, ["task create failed for `#{title}`: #{inspect(reason)}" | errors]}
        end
      end)

    if reply_to do
      response =
        cond do
          project_error ->
            "I couldn't create the GTD project: #{project_error}"

          task_errors != [] and created_task_ids == [] ->
            "Project created (`#{resolved_project_name}`), but task creation failed: #{Enum.join(Enum.reverse(task_errors), "; ")}"

          task_errors != [] ->
            "Created project `#{resolved_project_name}` (id: #{project_id || "unknown"}) and #{length(created_task_ids)} tasks. Some tasks failed: #{Enum.join(Enum.reverse(task_errors), "; ")}"

          true ->
            "Created project `#{resolved_project_name}` (id: #{project_id || "unknown"}) and #{length(created_task_ids)} task(s). Task IDs: #{Enum.join(Enum.reverse(created_task_ids), ", ")}"
        end

      reply_discord(reply_to, response)
    end
  end

  defp parse_goals_sort_option(text) do
    text_lower = String.downcase(text || "")

    cond do
      String.contains?(text_lower, "recent") ->
        :recent

      String.contains?(text_lower, "active") || String.contains?(text_lower, "momentum") ->
        :active

      true ->
        nil
    end
  end

  defp filter_goals_by_tenant(goals, tenant_id)
       when is_list(goals) and is_binary(tenant_id) and tenant_id != "" do
    Enum.filter(goals, fn goal ->
      case Map.get(goal, "tenant_id") || Map.get(goal, :tenant_id) do
        nil -> true
        ^tenant_id -> true
        _ -> false
      end
    end)
  end

  defp filter_goals_by_tenant(goals, _tenant_id) when is_list(goals), do: goals
  defp filter_goals_by_tenant(_goals, _tenant_id), do: []

  defp parse_goals_filter_label(text) do
    case Regex.scan(~r/#(\w+)/, text || "") do
      [[_, label] | _] -> label
      _ -> nil
    end
  end

  defp format_goal_labels(labels) do
    if labels && Enum.count(labels) > 0 do
      labels_str = Enum.map_join(labels, " ", &"##{&1}")
      " `#{labels_str}`"
    else
      ""
    end
  end

  defp format_last_decision(iso_string) do
    case DateTime.from_iso8601(iso_string || "") do
      {:ok, dt, _} ->
        days_ago = DateTime.diff(DateTime.utc_now(), dt, :second) / 86_400

        cond do
          days_ago < 1 -> "today"
          days_ago < 7 -> "#{round(days_ago)}d ago"
          true -> "#{round(days_ago / 7)}w ago"
        end

      _ ->
        "never"
    end
  end

  defp parse_skill_invocation(text) do
    # Match pattern: "skill_slug: payload text"
    case String.split(text, ":", parts: 2) do
      [potential_slug, payload] ->
        slug = String.trim(potential_slug)

        # Slug: lowercase letters, digits, underscore, hyphen (matches canonical skill slugs)
        if String.match?(slug, ~r/^[a-z0-9_-]+$/i) and String.length(slug) > 0 do
          {:skill, String.downcase(slug), String.trim(payload)}
        else
          :no_skill
        end

      _ ->
        :no_skill
    end
  end

  @doc false
  def skills_meta_intent(text) when is_binary(text) do
    normalized =
      text
      |> String.downcase()
      |> String.replace(~r/[[:punct:]]+/, " ")
      |> String.replace(~r/\s+/, " ")
      |> String.trim()

    cond do
      normalized in ["list skills", "skills list", "show skills", "what skills do you have"] ->
        :list_skills

      normalized in ["quick skills check", "skills check", "check skills"] ->
        :quick_check

      true ->
        :none
    end
  end

  def skills_meta_intent(_), do: :none

  defp maybe_handle_skills_meta_request(text, tenant_id, user_id, reply_to) do
    case skills_meta_intent(text) do
      :list_skills ->
        if reply_to,
          do: reply_discord(reply_to, build_skills_status_message(tenant_id, user_id, true))

        :handled

      :quick_check ->
        if reply_to,
          do: reply_discord(reply_to, build_skills_status_message(tenant_id, user_id, false))

        :handled

      :none ->
        :continue
    end
  end

  defp build_skills_status_message(tenant_id, user_id, include_guidance?) do
    case skills_runtime_status(tenant_id, user_id) do
      {:ok, :responder_online} ->
        base =
          "Skills runtime check: OK (skills command responder is reachable on NATS)."

        if include_guidance? do
          base <>
            "\nI can execute a specific skill via `<skill_slug>: <payload>`. " <>
            "For local filesystem or shell work on your machine, use `synapse_pi_go_delegate: <what pi-go should do>` " <>
            "(or `synapse-pi-go-delegate:`). " <>
            "For the daily agentic alignment scorecard (deterministic, no LLM), use `synapse_agentic_alignment_report:` " <>
            "(or `synapse-agentic-alignment-report:`). " <>
            "A skill catalog endpoint is not exposed in this environment."
        else
          base
        end

      {:error, reason} ->
        "Skills runtime check failed: #{reason}. I won't claim skills are loaded until this responder is reachable."
    end
  end

  defp skills_runtime_status(tenant_id, user_id) do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000),
         {:ok, json} <-
           Jason.encode(%{
             "event" => "bot.army.skills.command.__healthcheck__",
             "payload" => %{"text" => "ping"},
             "user_id" => user_id,
             "tenant_id" => tenant_id,
             "source" => "discord_synapse"
           }),
         {:ok, %{body: body}} <-
           Gnat.request(conn, "bot.army.skills.command.__healthcheck__", json, timeout: 3_000),
         {:ok, decoded} <- Jason.decode(body) do
      case decoded do
        %{"status" => "error", "error" => error} when is_binary(error) ->
          if String.contains?(String.downcase(error), "skill not found") do
            {:ok, :responder_online}
          else
            {:error, "skills responder returned error: #{error}"}
          end

        %{"status" => "success"} ->
          {:ok, :responder_online}

        _ ->
          {:error, "unexpected skills responder payload"}
      end
    else
      {:error, :timeout} ->
        {:error, "request timed out (no skills responder)"}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  defp process_skill_completion_dispatch_actions("synapse-gtd-create-safe", completion) do
    dispatch_actions = extract_dispatch_actions(completion)

    if dispatch_actions == [] do
      {completion, false}
    else
      results =
        dispatch_actions
        |> Enum.map(fn action -> execute_gtd_dispatch_action(action) end)
        |> Enum.filter(fn result -> result != nil end)

      if results == [] do
        {completion, false}
      else
        formatted_results =
          results
          |> Enum.map(fn
            {:project_created, id, _name} -> "project_id: #{id}"
            {:task_created, id, title} -> "task_id: #{id} - #{title}"
            _ -> nil
          end)
          |> Enum.reject(&is_nil/1)
          |> Enum.join("\n")

        {formatted_results, true}
      end
    end
  end

  defp process_skill_completion_dispatch_actions(_skill_slug, completion) do
    {completion, false}
  end

  defp extract_dispatch_actions(completion) when is_binary(completion) do
    # First try to extract "dispatch_action:" blocks
    explicit_actions =
      completion
      |> String.split("dispatch_action:")
      |> Enum.drop(1)
      |> Enum.map(&String.trim/1)
      |> Enum.map(&parse_dispatch_action_json/1)
      |> Enum.reject(&is_nil/1)

    if explicit_actions != [] do
      explicit_actions
    else
      # Fallback: try to extract JSON blocks from markdown code fences
      code_block_actions =
        case Regex.scan(~r/```json\n(.+?)\n```/s, completion, capture: :all_but_first) do
          [[json_str]] ->
            case Jason.decode(json_str) do
              {:ok, action} when is_map(action) -> [action]
              _ -> []
            end

          [[json_str] | rest] ->
            # Multiple JSON blocks
            Enum.concat(
              [
                case Jason.decode(json_str) do
                  {:ok, action} when is_map(action) -> [action]
                  _ -> []
                end
              ],
              Enum.map(rest, fn [s] ->
                case Jason.decode(s) do
                  {:ok, action} when is_map(action) -> [action]
                  _ -> []
                end
              end)
            )
            |> Enum.concat()

          _ ->
            []
        end

      if code_block_actions != [] do
        code_block_actions
      else
        # Fallback: try to find JSON objects directly in the text (raw JSON without code blocks)
        # Look for patterns like {"bridge_subject": ... or {"type": ...
        extract_json_objects(completion)
      end
    end
  end

  defp extract_json_objects(text) when is_binary(text) do
    # Find all positions where { appears
    positions =
      text
      |> String.graphemes()
      |> Enum.with_index()
      |> Enum.filter(fn {char, _} -> char == "{" end)
      |> Enum.map(fn {_, idx} -> idx end)

    # Try to parse JSON starting from each position
    Enum.flat_map(positions, fn start_pos ->
      substring = String.slice(text, start_pos..-1)

      case extract_json_from_string(substring) do
        {:ok, json_obj} ->
          case json_obj do
            %{"bridge_subject" => subject, "message" => message} when is_map(message) ->
              # Convert old action format to dispatch_action format
              case subject do
                "bridge.task.create" ->
                  [
                    %{
                      "type" => "gtd.task.create",
                      "params" => message
                    }
                  ]

                "bridge.project.create" ->
                  [
                    %{
                      "type" => "gtd.project.create",
                      "params" => message
                    }
                  ]

                _ ->
                  []
              end

            _ ->
              []
          end

        :error ->
          []
      end
    end)
    |> Enum.uniq()
  end

  defp extract_json_from_string(text) when is_binary(text) do
    # Try progressively longer substrings until we find valid JSON
    String.length(text)
    |> Range.new(10, -1)
    |> Enum.find_value(fn len ->
      try do
        substring = String.slice(text, 0, len)

        case Jason.decode(substring) do
          {:ok, obj} when is_map(obj) -> {:ok, obj}
          _ -> nil
        end
      rescue
        _ -> nil
      end
    end)
    |> case do
      {:ok, obj} -> {:ok, obj}
      _ -> :error
    end
  end

  defp extract_dispatch_actions(_), do: []

  defp parse_dispatch_action_json(json_str) when is_binary(json_str) do
    # Extract just the JSON object part
    case Regex.run(~r/^(\{.+\})/s, json_str, capture: :all_but_first) do
      [json_part] ->
        case Jason.decode(json_part) do
          {:ok, action} -> action
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp parse_dispatch_action_json(_), do: nil

  defp execute_gtd_dispatch_action(%{"type" => type, "params" => params}) when is_map(params) do
    case type do
      "gtd.project.create" ->
        project_name = Map.get(params, "name", "Untitled Project")
        description = Map.get(params, "description")

        bridge_payload =
          %{"name" => project_name, "description" => description}
          |> Map.reject(fn {_k, v} -> is_nil(v) end)

        case request_nats("bridge.project.create", bridge_payload) do
          {:ok, body} ->
            parsed = parse_body(body)

            project_id =
              get_in(parsed, ["data", "project_id"]) || get_in(parsed, ["data", "project", "id"])

            if is_binary(project_id) and project_id != "" do
              {:project_created, project_id, project_name}
            else
              nil
            end

          {:error, _reason} ->
            nil
        end

      "gtd.task.create" ->
        title = Map.get(params, "title", "Untitled")

        bridge_payload =
          %{
            "title" => title,
            "context" => Map.get(params, "context", "inbox"),
            "priority" => Map.get(params, "priority", "normal"),
            "description" => Map.get(params, "description"),
            "labels" => Map.get(params, "labels"),
            "project_id" => Map.get(params, "project_id"),
            "goal_id" => Map.get(params, "goal_id"),
            "parent_task_id" => Map.get(params, "parent_task_id")
          }
          |> Map.reject(fn {_k, v} -> is_nil(v) end)

        case request_nats("bridge.task.create", bridge_payload) do
          {:ok, body} ->
            parsed = parse_body(body)

            task_id =
              get_in(parsed, ["data", "task_id"]) || get_in(parsed, ["data", "task", "id"])

            if is_binary(task_id) and task_id != "" do
              {:task_created, task_id, title}
            else
              nil
            end

          {:error, _reason} ->
            nil
        end

      _ ->
        nil
    end
  end

  defp execute_gtd_dispatch_action(_), do: nil

  defp invoke_skill(
         skill_slug,
         payload_text,
         user_id,
         tenant_id,
         reply_to,
         session_id,
         original_text,
         channel_id
       ) do
    skill_slug = normalize_builtin_skill_slug(skill_slug)

    cond do
      skill_slug == "synapse_pi_go_delegate" ->
        run_synapse_pi_go_delegate_skill(
          payload_text,
          user_id,
          tenant_id,
          reply_to,
          session_id,
          original_text,
          channel_id
        )

      skill_slug == "synapse_agentic_alignment_report" ->
        case invoke_skill_direct(skill_slug, payload_text, user_id, tenant_id) do
          {:ok, completion} ->
            case maybe_validate_skill_completion(skill_slug, completion) do
              {:ok, validated_completion} ->
                Logger.info(
                  "[Orchestrator] Skill #{skill_slug} completed via direct skills call (deterministic)"
                )

                BotArmySynapse.ConversationStore.record_exchange(
                  session_id,
                  original_text,
                  validated_completion
                )

                if reply_to do
                  reply_discord(reply_to, validated_completion)
                end

              {:error, validation_reason} ->
                Logger.error(
                  "Skill #{skill_slug} direct completion failed validation: #{inspect(validation_reason)}"
                )

                if reply_to do
                  reply_discord(
                    reply_to,
                    "Skill request failed validation: #{inspect(validation_reason)}."
                  )
                end
            end

          {:error, reason} ->
            Logger.error("Skill #{skill_slug} direct call failed: #{inspect(reason)}")

            if reply_to do
              reply_discord(reply_to, "Skill request failed: #{inspect(reason)}")
            end
        end

      true ->
        case invoke_skill_via_llm_proxy(skill_slug, payload_text, user_id, tenant_id) do
          {:ok, completion} ->
            # For GTD creation skills, parse and execute dispatch_action blocks
            {processed_completion, has_dispatch_actions} =
              process_skill_completion_dispatch_actions(skill_slug, completion)

            if has_dispatch_actions do
              Logger.info("[Orchestrator] Skill #{skill_slug} included dispatch actions")
            end

            case maybe_validate_skill_completion(skill_slug, processed_completion) do
              {:ok, validated_completion} ->
                Logger.info("Skill #{skill_slug} completed via llm proxy")

                BotArmySynapse.ConversationStore.record_exchange(
                  session_id,
                  original_text,
                  validated_completion
                )

                if reply_to do
                  reply_discord(reply_to, validated_completion)
                end

              {:error, validation_reason} ->
                Logger.warning(
                  "Skill #{skill_slug} proxy completion failed validation: #{inspect(validation_reason)}"
                )

                fallback_to_direct_skill_call(
                  skill_slug,
                  payload_text,
                  user_id,
                  tenant_id,
                  reply_to,
                  session_id,
                  original_text,
                  {:proxy_validation_failed, validation_reason},
                  channel_id
                )
            end

          {:error, proxy_reason} ->
            fallback_to_direct_skill_call(
              skill_slug,
              payload_text,
              user_id,
              tenant_id,
              reply_to,
              session_id,
              original_text,
              proxy_reason,
              channel_id
            )
        end
    end
  end

  defp fallback_to_direct_skill_call(
         skill_slug,
         payload_text,
         user_id,
         tenant_id,
         reply_to,
         session_id,
         original_text,
         proxy_reason,
         _channel_id
       ) do
    Logger.warning(
      "Skill #{skill_slug} via llm proxy failed, falling back to direct skills call: #{inspect(proxy_reason)}"
    )

    case invoke_skill_direct(skill_slug, payload_text, user_id, tenant_id) do
      {:ok, completion} ->
        case maybe_validate_skill_completion(skill_slug, completion) do
          {:ok, validated_completion} ->
            Logger.info("Skill #{skill_slug} completed via direct skills call")

            BotArmySynapse.ConversationStore.record_exchange(
              session_id,
              original_text,
              validated_completion
            )

            if reply_to do
              reply_discord(reply_to, validated_completion)
            end

          {:error, validation_reason} ->
            Logger.error(
              "Skill #{skill_slug} direct completion failed validation: #{inspect(validation_reason)}"
            )

            if reply_to do
              reply_discord(
                reply_to,
                "Skill request failed validation: #{inspect(validation_reason)}. No GTD item was created."
              )
            end
        end

      {:error, reason} ->
        Logger.error(
          "Skill #{skill_slug} failed after proxy + direct attempts: #{inspect(reason)} (proxy=#{inspect(proxy_reason)})"
        )

        if reply_to do
          reply_discord(
            reply_to,
            "Skill request failed: proxy=#{inspect(proxy_reason)} direct=#{inspect(reason)}"
          )
        end
    end
  end

  defp maybe_validate_skill_completion("synapse-gtd-create-safe", completion),
    do: validate_gtd_create_completion(completion)

  defp maybe_validate_skill_completion("synapse_gtd_create_safe", completion),
    do: validate_gtd_create_completion(completion)

  defp maybe_validate_skill_completion(_skill_slug, completion), do: {:ok, completion}

  @doc """
  Detects Syn self-delegation lines for pi-go (Discord LLM path).

  Looks for a line `synapse_pi_go_delegate: <task>` (case-insensitive). When present,
  Synapse dispatches `<task>` to pi-go instead of echoing the raw model line to Discord.
  """
  def parse_pi_go_delegate_directive(text) when is_binary(text) do
    text
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.find_value(fn line ->
      case Regex.run(~r/^synapse_pi_go_delegate:\s*(.+)$/i, line) do
        [_, rest] ->
          rest = String.trim(rest)
          if rest != "", do: {:delegate, rest}, else: nil

        _ ->
          nil
      end
    end)
    |> case do
      {:delegate, _} = found -> found
      _ -> :none
    end
  end

  def parse_pi_go_delegate_directive(_), do: :none

  defp normalize_builtin_skill_slug(slug) when is_binary(slug) do
    slug = String.trim(slug)

    case String.downcase(slug) do
      "synapse-pi-go-delegate" -> "synapse_pi_go_delegate"
      "synapse-agentic-alignment-report" -> "synapse_agentic_alignment_report"
      _ -> slug
    end
  end

  defp normalize_builtin_skill_slug(_), do: ""

  defp run_synapse_pi_go_delegate_skill(
         payload_text,
         user_id,
         tenant_id,
         reply_to,
         session_id,
         original_text,
         channel_id
       ) do
    prompt = String.trim(payload_text || "")

    if prompt == "" do
      msg =
        "synapse_pi_go_delegate needs text after the colon describing what pi-go should run on your machine."

      if reply_to, do: reply_discord(reply_to, msg)
    else
      params = %{
        "command" => "run",
        "prompt" => prompt,
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "correlation_id" => UUID.uuid4(),
        "discord_channel_id" => channel_id
      }

      ack =
        case dispatch_pi_go_command(params) do
          %{"status" => "accepted", "data" => data} ->
            cid = Map.get(data, "correlation_id", "unknown")

            "Delegated to pi-go (correlation_id=#{cid}). Local tools run there; watch this channel for pi-go completed/failed."

          %{"status" => "error", "error" => err} ->
            "Could not queue pi-go: #{inspect(err)}"

          other ->
            "Unexpected pi-go dispatch result: #{inspect(other)}"
        end

      BotArmySynapse.ConversationStore.record_exchange(session_id, original_text, ack)

      if reply_to, do: reply_discord(reply_to, ack)
    end
  end

  @doc false
  def validate_gtd_create_completion(completion) when is_binary(completion) do
    Logger.debug(
      "[validate_gtd_create_completion] Validating completion: #{String.slice(completion, 0, 200)}"
    )

    id_lines = Regex.scan(~r/\b(?:task_id|project_id)\s*:\s*([^\s]+)/i, completion)

    ids =
      id_lines
      |> Enum.map(fn [_, id] -> String.trim(id) end)
      |> Enum.reject(&(&1 == ""))

    Logger.debug("[validate_gtd_create_completion] Extracted IDs: #{inspect(ids)}")

    cond do
      ids == [] ->
        Logger.warning("[validate_gtd_create_completion] No IDs found in completion")
        {:error, :missing_ids}

      Enum.all?(ids, &valid_uuid?/1) ->
        Logger.info("[validate_gtd_create_completion] Valid UUIDs found, validation passed")
        {:ok, completion}

      true ->
        Logger.warning("[validate_gtd_create_completion] Invalid ID format: #{inspect(ids)}")
        {:error, :invalid_id_format}
    end
  end

  def validate_gtd_create_completion(_), do: {:error, :invalid_completion}

  defp valid_uuid?(id) when is_binary(id) do
    String.match?(
      id,
      ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
    )
  end

  defp invoke_skill_via_llm_proxy(skill_slug, payload_text, user_id, tenant_id) do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000) do
      timeout_ms = 95_000

      request = %{
        "event" => "llm.skill.execute",
        "event_id" => UUID.uuid4(),
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "schema_version" => "1.0",
        "source" => "discord_synapse",
        "source_node" => node() |> Atom.to_string(),
        "triggered_by" => "discord_message",
        "tenant_id" => tenant_id,
        "user_id" => user_id,
        "timeout_ms" => timeout_ms,
        "payload" => %{
          "slug" => skill_slug,
          "payload_text" => payload_text,
          "tenant_id" => tenant_id,
          "user_id" => user_id,
          "source" => "discord_synapse",
          "triggered_by" => "discord_message"
        }
      }

      with {:ok, json} <- Jason.encode(request),
           {:ok, %{body: response_body}} <-
             Gnat.request(conn, "llm.skill.execute", json, timeout: timeout_ms + 5_000),
           {:ok, decoded} <- Jason.decode(response_body) do
        case decoded do
          %{"status" => "success", "completion" => completion} when is_binary(completion) ->
            {:ok, completion}

          %{"status" => "error", "error" => error} ->
            {:error, {:llm_proxy_error, error}}

          other ->
            {:error, {:unexpected_proxy_response, other}}
        end
      else
        {:error, reason} -> {:error, {:llm_proxy_request_failed, reason}}
      end
    else
      {:error, reason} -> {:error, {:nats_connection_failed, reason}}
    end
  end

  defp invoke_skill_direct(skill_slug, payload_text, user_id, tenant_id) do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000) do
      envelope = %{
        "event" => "bot.army.skills.command.#{skill_slug}",
        "event_id" => UUID.uuid4(),
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
        "schema_version" => "1.0",
        "payload" => %{"text" => payload_text},
        "user_id" => user_id,
        "tenant_id" => tenant_id,
        "source" => "discord_synapse",
        "source_node" => node() |> Atom.to_string(),
        "triggered_by" => "discord_message"
      }

      with {:ok, json} <- Jason.encode(envelope),
           {:ok, %{body: response_body}} <-
             Gnat.request(conn, "bot.army.skills.command.#{skill_slug}", json, timeout: 30_000),
           {:ok, decoded} <- Jason.decode(response_body) do
        case decoded do
          %{"status" => "success", "payload" => %{"completion" => completion}} ->
            {:ok, completion}

          %{"status" => "error", "error" => error} ->
            {:error, {:skills_error, error}}

          other ->
            {:error, {:unexpected_skills_response, other}}
        end
      else
        {:error, reason} -> {:error, {:skills_request_failed, reason}}
      end
    else
      {:error, reason} -> {:error, {:nats_connection_failed, reason}}
    end
  end

  defp build_discord_prompt(text, author_name, command, context_data, conversation_history) do
    context_parts = format_context_parts(context_data)

    command_context =
      if command do
        "\nCommand: #{command}"
      else
        ""
      end

    history_section =
      case conversation_history do
        [] ->
          ""

        _ ->
          pairs =
            Enum.map_join(conversation_history, "\n", fn exchange ->
              "  Q: #{exchange.question}\n  A: #{String.slice(exchange.answer, 0, 150)}"
            end)

          "\n\n## Conversation History\n#{pairs}"
      end

    persona = get_discord_persona()

    """
    #{persona}

    User: #{author_name}#{command_context}
    Message: #{text}

    #{context_parts}#{history_section}

    Respond to the user's message. Be conversational but concise.
    """
  end

  defp get_discord_persona do
    Application.get_env(:bot_army_synapse, :discord_persona) ||
      """
      You are Syn, an AI assistant in the Bot Army ecosystem. Be concise, direct, and friendly.
      Respond in plain text (no markdown formatting that Discord won't render well).
      This Discord reply path has no access to the operator's host filesystem, shell, or git repos.
      Never claim you created, edited, deleted, or read a local file, and never claim you ran a command on the user's machine.
      If the user asks for a local file write/read, shell command, repo mutation, or anything that requires pi-go tools on their machine,
      respond with a single line only, exactly in this form (no other text before or after):
      synapse_pi_go_delegate: <verbatim task for pi-go, including paths and file contents if writing a file>
      For NATS design questions (not execution), explain that Synapse uses platform-mediated NATS request/publish paths.
      Keep normal answers under 500 characters when possible.
      """
  end

  defp format_context_parts(context_data) do
    context_data
    |> Enum.map(fn
      {:gtd, data} when is_list(data) and length(data) > 0 ->
        "## GTD Tasks (#{length(data)} tasks)\n" <>
          Enum.map_join(Enum.take(data, 5), "\n", fn t ->
            "  - #{t["title"]} [#{t["priority"]}] #{t["status"]}"
          end)

      {:gtd, _} ->
        nil

      {:calendar, data} when is_list(data) and data != [] ->
        "## Upcoming Events (#{Enum.count(data)})\n" <>
          Enum.map_join(Enum.take(data, 3), "\n", fn e ->
            "  - #{e["summary"]} at #{e["start_time"]}"
          end)

      {:calendar, _} ->
        nil

      {:context_state, data} ->
        "## Current Mode: #{Map.get(data, "mode", "unknown")}"

      {:time, data} ->
        "#{Map.get(data, "day_of_week", "")} #{Map.get(data, "hour", "")}:00"

      {:fitness, data} when is_map(data) and map_size(data) > 0 ->
        "## Fitness\n#{inspect(data)}"

      {:chore, data} when is_map(data) and map_size(data) > 0 ->
        "## Chores\n#{inspect(data)}"

      {:job, data} when is_map(data) and map_size(data) > 0 ->
        "## Jobs: #{Map.get(data, "listings", "?")} listings, #{Map.get(data, "applications", "?")} applications"

      {:terrain, data} when is_map(data) and map_size(data) > 0 ->
        "## Terrain\n#{inspect(data)}"

      {:sre, data} when is_map(data) and map_size(data) > 0 ->
        "## SRE: #{inspect(data)}"

      {:advocacy, data} when is_map(data) and map_size(data) > 0 ->
        "## Advocacy\n#{inspect(data)}"

      {:history, events} when is_list(events) and events != [] ->
        "## Recent\n" <>
          Enum.map_join(Enum.take(events, 10), ", ", fn e ->
            "#{e.event_type}: #{e.summary}"
          end)

      {:fleet, data} when is_map(data) and map_size(data) > 0 ->
        format_fleet_for_discord(data)

      {:internal_docs, data} when is_map(data) ->
        case Map.get(data, "results") do
          results when is_list(results) and results != [] ->
            q = Map.get(data, "query", "")
            fb = if(f = Map.get(data, "fallback"), do: " [#{f}]", else: "")

            "## Internal docs (#{String.slice(q, 0, 120)})#{fb}\n" <>
              Enum.map_join(Enum.take(results, 3), "\n", &format_internal_docs_hit/1)

          _ ->
            nil
        end

      _ ->
        nil
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.join("\n\n")
  end

  defp format_internal_docs_hit(r) when is_map(r) do
    BotArmySynapse.Context.InternalDocs.format_hit_for_prompt(r)
  end

  defp format_fleet_for_discord(data) do
    online_count = Map.get(data, "online_count", 0)
    stale_count = Map.get(data, "stale_count", 0)
    down_count = Map.get(data, "down_count", 0)

    parts =
      ["Fleet: #{online_count} online"]
      |> maybe_add_count(stale_count, "stale")
      |> maybe_add_count(down_count, "down")

    Enum.join(parts, ", ")
  end

  defp format_fleet_for_prompt(data) do
    online = Map.get(data, "online", [])
    stale = Map.get(data, "stale", [])
    down = Map.get(data, "down", [])

    online_names = Enum.map_join(online, ", ", & &1)

    stale_part =
      case stale do
        [] ->
          ""

        _ ->
          entries =
            Enum.map_join(stale, ", ", fn {name, sec} -> "#{name} (#{div(sec, 60)}min ago)" end)

          "\nStale: #{entries}"
      end

    down_part =
      case down do
        [] -> ""
        _ -> "\nDown: #{Enum.map_join(down, ", ", & &1)}"
      end

    "## Fleet Status\nOnline: #{online_names} (#{length(online)} bots)#{stale_part}#{down_part}"
  end

  defp maybe_add_count(parts, 0, _label), do: parts
  defp maybe_add_count(parts, count, label), do: parts ++ ["#{count} #{label}"]

  @doc false
  def call_llm_sync(prompt, opts \\ []) do
    request_ctx = Keyword.get(opts, :request_ctx, %{})

    case request_llm_sync(prompt, request_ctx, 30_000) do
      {:ok, _} = ok ->
        ok

      {:error, reason} ->
        Logger.warning("Primary LLM request failed: #{inspect(reason)}; retrying once")

        case request_llm_sync(prompt, request_ctx, 45_000) do
          {:ok, _} = ok ->
            ok

          {:error, retry_reason} ->
            maybe_degraded_llm_result(prompt, retry_reason)
        end
    end
  end

  defp request_llm_sync(prompt, request_ctx, timeout_ms) do
    conn = GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000)
    tenant_id = Map.get(request_ctx, :tenant_id) || BotArmyRuntime.Tenant.default_tenant_id()
    user_id = Map.get(request_ctx, :user_id)
    run_id = Map.get(request_ctx, :run_id)

    case conn do
      {:ok, gnat} ->
        request_id = UUID.uuid4()
        request_type = normalize_llm_request_type(Map.get(request_ctx, :request_type))
        model_preference = normalize_llm_model_preference(Map.get(request_ctx, :model_preference))

        lane =
          normalize_llm_lane(Map.get(request_ctx, :llm_lane) || Map.get(request_ctx, :priority))

        subject = llm_subject_for_lane(lane)
        degrade_ok = lane != "urgent"
        reply_subject = "llm.response.#{request_id}"

        payload =
          Jason.encode!(%{
            "request_id" => request_id,
            "request_type" => request_type,
            "prompt_context" => %{"prompt" => prompt},
            # Compatibility fallback for older LLM consumers still reading "text".
            "text" => prompt,
            "model_preference" => model_preference,
            "reply_subject" => reply_subject,
            "timeout_ms" => timeout_ms,
            "priority" => lane,
            "deadline_ms" => timeout_ms,
            "degrade_ok" => degrade_ok,
            "tenant_id" => tenant_id,
            "user_id" => user_id,
            "run_id" => run_id,
            "source" => "bot_army_synapse"
          })

        case Gnat.request(gnat, subject, payload, timeout: timeout_ms) do
          {:ok, response} ->
            body = extract_body(response)

            case Jason.decode(body) do
              {:ok, decoded} ->
                text = extract_llm_text(decoded)
                error = extract_llm_error(decoded)

                cond do
                  text != "" -> {:ok, text}
                  error != "" -> {:error, {:llm_error, error}}
                  true -> {:error, :empty_response}
                end

              {:error, _} ->
                {:error, :decode_failed}
            end

          {:error, reason} ->
            {:error, reason}

          {:timeout, _} ->
            {:error, :timeout}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reply_discord(reply_to, text) do
    response = Jason.encode!(%{"response" => text})

    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      Gnat.pub(conn, reply_to, response)
    end
  end

  defp extract_body(%{body: body}), do: body
  defp extract_body(body) when is_binary(body), do: body

  @doc false
  def extract_llm_text(decoded) when is_map(decoded) do
    Map.get(decoded, "completion") ||
      Map.get(decoded, "response") ||
      Map.get(decoded, "text") ||
      Map.get(decoded, "content") ||
      Map.get(decoded, "result") ||
      Map.get(decoded, "recommendation") ||
      get_in(decoded, ["payload", "completion"]) ||
      get_in(decoded, ["payload", "response"]) ||
      get_in(decoded, ["payload", "text"]) ||
      get_in(decoded, ["payload", "content"]) ||
      get_in(decoded, ["payload", "result"]) ||
      get_in(decoded, ["payload", "recommendation"]) ||
      get_in(decoded, ["data", "completion"]) ||
      get_in(decoded, ["data", "response"]) ||
      get_in(decoded, ["data", "text"]) ||
      get_in(decoded, ["data", "content"]) ||
      get_in(decoded, ["data", "result"]) ||
      get_in(decoded, ["data", "recommendation"]) ||
      ""
  end

  def extract_llm_text(_), do: ""

  @doc false
  def extract_llm_error(decoded) when is_map(decoded) do
    Map.get(decoded, "error") ||
      Map.get(decoded, "message") ||
      get_in(decoded, ["payload", "error"]) ||
      get_in(decoded, ["payload", "message"]) ||
      get_in(decoded, ["data", "error"]) ||
      get_in(decoded, ["data", "message"]) ||
      ""
  end

  def extract_llm_error(_), do: ""

  defp normalize_llm_lane(nil), do: "interactive"

  defp normalize_llm_lane(lane) when is_atom(lane) do
    normalize_llm_lane(Atom.to_string(lane))
  end

  defp normalize_llm_lane(lane) when is_binary(lane) do
    case String.downcase(String.trim(lane)) do
      "urgent" -> "urgent"
      "high" -> "urgent"
      "interactive" -> "interactive"
      "normal" -> "interactive"
      "background" -> "background"
      "low" -> "background"
      _ -> "interactive"
    end
  end

  defp llm_subject_for_lane("urgent"), do: "pi-go.llm.request.chat.urgent"
  defp llm_subject_for_lane("background"), do: "pi-go.llm.request.chat.background"
  defp llm_subject_for_lane(_), do: "pi-go.llm.request.chat.interactive"

  defp maybe_degraded_llm_result(prompt, reason) do
    if llm_capacity_error?(reason) do
      {:ok, deterministic_degraded_text(prompt, reason)}
    else
      {:error, reason}
    end
  end

  defp llm_capacity_error?(:no_providers_available), do: true
  defp llm_capacity_error?(:provider_not_configured), do: true
  defp llm_capacity_error?(:timeout), do: true

  defp llm_capacity_error?({:llm_error, error}) when is_binary(error) do
    lower = String.downcase(error)

    String.contains?(lower, "no_providers_available") or
      String.contains?(lower, "provider_not_configured") or
      String.contains?(lower, "timeout")
  end

  defp llm_capacity_error?(_), do: false

  defp deterministic_degraded_text(prompt, reason) when is_binary(prompt) do
    down = String.downcase(prompt)

    if String.contains?(down, "file writing") or
         String.contains?(down, "write file") or
         String.contains?(down, "write capability") do
      "LLM providers are temporarily unavailable (#{inspect(reason)}). " <>
        "I can still answer from deterministic checks: file writing capability depends on the agent runtime/tooling layer, " <>
        "not the LLM itself. Please run a direct file write/read probe in your current runtime and retry."
    else
      "LLM providers are temporarily unavailable (#{inspect(reason)}). " <>
        "I can continue with deterministic context checks and partial replies while model-backed generation recovers."
    end
  end

  defp deterministic_degraded_text(_prompt, reason) do
    "LLM providers are temporarily unavailable (#{inspect(reason)}). Please retry shortly."
  end

  @doc """
  Handle a Claude Bridge result arriving on `bot_army.claude.result.{session_id}`.

  Publishes an observability event so other bots can react to Claude's output.

  When `triggered_by` is `"brain"` (DecisionEngine → `bot_army.claude.trigger.brain`)
  and the payload lists `goal_names`, records a decision per matching goal via
  `GoalStore.record_decision/2` so goal-at-risk / goal-centric triggers close the loop.
  """
  def handle_claude_result(message) do
    payload = message["payload"] || %{}
    status = payload["status"] || "unknown"
    session_id = message["event_id"] || "unknown"
    result = payload["result"]
    triggered_by = message["triggered_by"] || "unknown"

    Logger.info(
      "[Orchestrator] Claude result received: session=#{String.slice(session_id, 0, 8)}... status=#{status} triggered_by=#{triggered_by}"
    )

    maybe_record_brain_goal_decisions(triggered_by, status, payload, result)

    # Publish observability event for other bots
    event_data = %{
      "event" => "events.synapse.claude.result",
      "event_id" => UUID.uuid4(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "source_node" => node() |> Atom.to_string(),
      "triggered_by" => triggered_by,
      "schema_version" => "1.0",
      "payload" => %{
        "session_id" => session_id,
        "status" => status,
        "result_summary" => if(result, do: String.slice(result, 0, 500), else: nil),
        "cost_usd" => payload["cost_usd"],
        "duration_ms" => payload["duration_ms"],
        "model" => payload["model"],
        "launcher" => payload["launcher"]
      }
    }

    case publish_nats("events.synapse.claude.result", event_data) do
      :ok ->
        Logger.debug("[Orchestrator] Published Claude result observability event")

      {:error, reason} ->
        Logger.warning("[Orchestrator] Failed to publish Claude result event: #{inspect(reason)}")
    end

    :ok
  end

  defp maybe_record_brain_goal_decisions("brain", status, payload, result_text)
       when status in ["completed", "success", "ok"] do
    goal_names =
      case Map.get(payload, "goal_names", []) do
        list when is_list(list) -> list
        _ -> []
      end

    if goal_names == [] do
      :ok
    else
      summary =
        if is_binary(result_text) and result_text != "" do
          "Brain session: " <> String.slice(result_text, 0, 500)
        else
          "Brain session completed (no result text)"
        end

      for name <- goal_names, is_binary(name) and name != "" do
        case BotArmySynapse.GoalStore.get_goal_by_name(String.trim(name)) do
          %{"id" => project_id} when is_binary(project_id) ->
            BotArmySynapse.GoalStore.record_decision(project_id, summary)
            Logger.info("[Orchestrator] Recorded goal decision for project_id=#{project_id}")

          _ ->
            Logger.debug(
              "[Orchestrator] No cached goal named #{inspect(name)} — skipping decision record"
            )
        end
      end

      :ok
    end
  end

  defp maybe_record_brain_goal_decisions("brain", status, _payload, _result) do
    Logger.debug("[Orchestrator] Brain session: not recording goals (status=#{inspect(status)})")
    :ok
  end

  defp maybe_record_brain_goal_decisions(_triggered_by, _status, _payload, _result), do: :ok

  @doc """
  Handle synchronous Claude query via request/reply.

  Expects payload with: question, current_focus (optional), context (optional), session_id (optional),
  context_sources (optional, defaults to GTD+Calendar+Context+Time).

  Replies via reply_to with JSON: {"status": "success", "response": "..."} or
  {"status": "error", "error": "reason"}
  """
  def handle_claude_query(payload, reply_to) do
    question = Map.get(payload, "question", "")
    current_focus = Map.get(payload, "current_focus")
    extra_context = Map.get(payload, "context", %{})
    session_id = Map.get(payload, "session_id", "claude_default")
    context_sources = Map.get(payload, "context_sources", default_context_sources() ++ ["time"])
    query_type = Map.get(payload, "type")
    request_ctx = extract_request_context(payload, Map.get(payload, "payload", %{}))

    Logger.info("[Orchestrator] Claude query: #{String.slice(question, 0, 60)}...")

    cond do
      # Handle fleet status queries directly without LLM
      query_type == "fleet" or String.contains?(String.downcase(question), ["fleet", "status"]) ->
        handle_fleet_status_query(reply_to)

      # Handle goal/project queries directly without LLM
      query_type == "goals" or String.contains?(String.downcase(question), ["goal", "project"]) ->
        handle_goals_query(reply_to)

      true ->
        context_data = gather_context(context_sources, question)
        conversation_history = BotArmySynapse.ConversationStore.get_history(session_id)

        prompt =
          build_claude_prompt(
            question,
            current_focus,
            extra_context,
            context_data,
            conversation_history
          )

        case call_llm_sync(prompt, request_ctx: request_ctx) do
          {:ok, response} ->
            BotArmySynapse.ConversationStore.record_exchange(session_id, question, response)

            if reply_to do
              reply_nats_json(reply_to, Reply.ok(%{"response" => response}))
            end

          {:error, reason} ->
            Logger.warning("[Orchestrator] Claude query LLM failed: #{inspect(reason)}")

            if reply_to do
              reply_nats_json(reply_to, Reply.error(inspect(reason), :query_failed))
            end
        end
    end
  end

  defp handle_fleet_status_query(reply_to) do
    fleet_data = Fleet.get_context()

    if reply_to do
      reply_nats_json(reply_to, %{
        "status" => "success",
        "fleet" => fleet_data,
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    end
  end

  defp handle_goals_query(reply_to) do
    Logger.info("[handle_goals_query] Called with reply_to: #{inspect(reply_to)}")
    goals = BotArmySynapse.GoalStore.list_goals(:all)
    Logger.info("[handle_goals_query] Goals retrieved: #{length(goals || [])} goals")

    if reply_to do
      goals_list = goals || []

      completion =
        if goals_list != [] do
          goals_text =
            Enum.map_join(goals_list, "\n", fn g ->
              "• #{g["name"]} (#{g["status"]})"
            end)

          "You have #{Enum.count(goals_list)} active goals:\n\n#{goals_text}"
        else
          "You don't have any goals set yet. Would you like to create some?"
        end

      reply_nats_json(reply_to, %{
        "completion" => completion,
        "goals" => goals_list,
        "count" => length(goals_list),
        "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
      })
    end
  end

  @doc """
  Handle asynchronous Claude analysis via fire-and-forget.

  Gathers context, calls LLM, publishes result to events.synapse.claude.result.<event_id>
  """
  def handle_llm_analyze(payload, event_id) do
    payload = unwrap_payload(payload)
    request_ctx = extract_request_context(payload, payload)
    run_id = event_id || request_ctx.run_id
    question = Map.get(payload, "question", "")
    current_focus = Map.get(payload, "current_focus")
    extra_context = Map.get(payload, "context", %{})
    session_id = Map.get(payload, "session_id", "claude_default")
    context_sources = Map.get(payload, "context_sources", default_context_sources() ++ ["time"])
    tenant_id = request_ctx.tenant_id
    user_id = request_ctx.user_id

    Logger.info(
      "[Orchestrator] Claude analyze (#{String.slice(event_id, 0, 8)}...): #{String.slice(question, 0, 60)}..."
    )

    track_run_start(run_id, "synapse.analyze", %{
      "tenant_id" => tenant_id,
      "user_id" => user_id,
      "session_id" => session_id,
      "question" => question
    })

    publish_synapse_progress(%{
      "run_id" => run_id,
      "step" => "preflight_started",
      "tenant_id" => tenant_id,
      "user_id" => user_id
    })

    publish_synapse_progress(%{
      "run_id" => run_id,
      "step" => "context_lookup_started",
      "tenant_id" => tenant_id,
      "user_id" => user_id,
      "context_sources" => context_sources
    })

    context_data = gather_context(context_sources, question)
    context_summary = format_context_summary(context_data)

    publish_synapse_progress(%{
      "run_id" => run_id,
      "step" => "context_lookup_completed",
      "tenant_id" => tenant_id,
      "user_id" => user_id,
      "context_summary" => context_summary
    })

    conversation_history = BotArmySynapse.ConversationStore.get_history(session_id)

    prompt =
      build_claude_prompt(
        question,
        current_focus,
        extra_context,
        context_data,
        conversation_history
      )

    publish_synapse_progress(%{
      "run_id" => run_id,
      "step" => "llm_request_started",
      "tenant_id" => tenant_id,
      "user_id" => user_id
    })

    case call_llm_sync(prompt,
           request_ctx: %{tenant_id: tenant_id, user_id: user_id, run_id: run_id}
         ) do
      {:ok, response} ->
        publish_synapse_progress(%{
          "run_id" => run_id,
          "step" => "finalizing_response",
          "tenant_id" => tenant_id,
          "user_id" => user_id
        })

        BotArmySynapse.ConversationStore.record_exchange(session_id, question, response)
        publish_analyze_result(event_id, response, :success)

        publish_synapse_progress(%{
          "run_id" => run_id,
          "step" => "completed",
          "tenant_id" => tenant_id,
          "user_id" => user_id
        })

        BotArmySynapse.RunStore.mark_status(run_id, "completed", %{
          "last_response" => response,
          "last_response_at" => DateTime.utc_now() |> DateTime.to_iso8601()
        })

      {:error, reason} ->
        Logger.warning("[Orchestrator] Claude analyze LLM failed: #{inspect(reason)}")
        publish_analyze_result(event_id, inspect(reason), :error)

        publish_synapse_progress(%{
          "run_id" => run_id,
          "step" => "failed",
          "tenant_id" => tenant_id,
          "user_id" => user_id,
          "error" => inspect(reason)
        })

        BotArmySynapse.RunStore.mark_status(run_id, "failed", %{
          "error" => inspect(reason)
        })
    end
  end

  defp build_claude_prompt(
         question,
         current_focus,
         extra_context,
         context_data,
         conversation_history
       ) do
    context_parts = format_context_parts(context_data)

    focus_section =
      if current_focus do
        "\n## Current Focus\n#{current_focus}"
      else
        ""
      end

    extra_section =
      if Map.keys(extra_context) |> length() > 0 do
        extra_text =
          extra_context
          |> Enum.map(fn {k, v} -> "  - #{k}: #{inspect(v)}" end)
          |> Enum.join("\n")

        "\n## Additional Context\n#{extra_text}"
      else
        ""
      end

    history_section =
      case conversation_history do
        [] ->
          ""

        _ ->
          pairs =
            Enum.map_join(conversation_history, "\n", fn exchange ->
              "  Q: #{exchange.question}\n  A: #{String.slice(exchange.answer, 0, 200)}"
            end)

          "\n\n## Conversation History\n#{pairs}"
      end

    """
    You are Synapse, an intelligent context aggregator assisting Claude Code.
    Provide strategic guidance and actionable insights based on the context from multiple systems.
    Be concise and focused on what's most important right now.

    Question: #{question}

    #{context_parts}#{focus_section}#{extra_section}#{history_section}

    Provide a clear, actionable response that incorporates the context to help Claude decide
    what to work on next or how to approach the current task.
    """
  end

  defp reply_nats_json(reply_to, payload) do
    with {:ok, json} <- Jason.encode(payload),
         {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000) do
      headers = BotArmyRuntime.Tracing.inject_trace_context([])
      Gnat.pub(conn, reply_to, json, headers: headers)
    else
      {:error, reason} ->
        Logger.error("[Orchestrator] Failed to reply to #{reply_to}: #{inspect(reason)}")
    end
  end

  defp publish_analyze_result(event_id, response, status) do
    BotArmyCore.NATS.publish("events.synapse.claude.result.#{event_id}", %{
      "status" => Atom.to_string(status),
      "response" => response,
      "event_id" => event_id,
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601()
    })
  rescue
    _ -> :ok
  end

  # --- Workflow Dispatch ---

  def dispatch_workflow(payload, reply_to) do
    payload = unwrap_payload(payload)
    request_ctx = extract_request_context(payload, payload)
    run_id = request_ctx.run_id
    tenant_id = request_ctx.tenant_id
    user_id = request_ctx.user_id
    actions = Map.get(payload, "actions", [])

    track_run_start(run_id, "synapse.workflow", %{
      "tenant_id" => tenant_id,
      "user_id" => user_id,
      "action_count" => length(actions),
      "reply_to" => reply_to,
      "payload" => payload
    })

    publish_synapse_progress(%{
      "run_id" => run_id,
      "step" => "workflow_started",
      "tenant_id" => tenant_id,
      "user_id" => user_id,
      "action_count" => length(actions)
    })

    results =
      actions
      |> Task.async_stream(&dispatch_action/1, timeout: 8_000, on_timeout: :kill_task)
      |> Enum.map(fn
        {:ok, result} -> result
        {:exit, :timeout} -> %{"status" => "error", "error" => "timeout"}
        {:exit, reason} -> %{"status" => "error", "error" => inspect(reason)}
      end)

    error_count = Enum.count(results, &(&1["status"] == "error"))

    response = %{
      "status" => if(error_count == 0, do: "ok", else: "partial"),
      "results" => results,
      "errors" => error_count
    }

    publish_synapse_progress(%{
      "run_id" => run_id,
      "step" => "workflow_completed",
      "tenant_id" => tenant_id,
      "user_id" => user_id,
      "errors" => error_count
    })

    BotArmySynapse.RunStore.mark_status(
      run_id,
      if(error_count == 0, do: "completed", else: "partial"),
      %{
        "errors" => error_count,
        "results" => results
      }
    )

    if reply_to do
      with {:ok, conn} <-
             GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000),
           {:ok, json} <- Jason.encode(response) do
        Gnat.pub(conn, reply_to, json)
      end
    end
  end

  def handle_continue(payload, reply_to) do
    request = unwrap_payload(payload)
    run_id = Map.get(request, "run_id", "")

    case BotArmySynapse.RunStore.get_run(run_id) do
      nil ->
        publish_synapse_progress(%{
          "run_id" => run_id,
          "step" => "resume_failed",
          "error" => "run_not_found"
        })

        if reply_to do
          reply_nats_json(reply_to, Reply.error("run_not_found", :not_found))
        end

      run ->
        route = Map.get(run, "route")
        BotArmySynapse.RunStore.mark_status(run_id, "resumed")

        publish_synapse_progress(%{
          "run_id" => run_id,
          "step" => "resumed",
          "tenant_id" => run["tenant_id"],
          "user_id" => run["user_id"],
          "route" => route
        })

        started? =
          case route do
            "synapse.analyze" ->
              replay =
                %{
                  "question" => run["question"] || "",
                  "session_id" => run["session_id"],
                  "tenant_id" => run["tenant_id"],
                  "user_id" => run["user_id"],
                  "context_sources" =>
                    Map.get(request, "context_sources", default_context_sources() ++ ["time"])
                }
                |> Map.merge(Map.get(request, "overrides", %{}))

              spawn(fn -> handle_llm_analyze(replay, run_id) end)
              true

            "synapse.workflow" ->
              replay =
                (run["payload"] || %{})
                |> Map.merge(Map.get(request, "overrides", %{}))
                |> Map.put("run_id", run_id)

              spawn(fn -> dispatch_workflow(replay, reply_to || run["reply_to"]) end)
              true

            "llm.context.analyze" ->
              replay =
                %{
                  "event_id" => run_id,
                  "payload" => %{
                    "question" => run["question"] || "",
                    "tenant_id" => run["tenant_id"],
                    "user_id" => run["user_id"],
                    "session_id" => run["session_id"],
                    "context_sources" =>
                      Map.get(request, "context_sources", default_context_sources())
                  },
                  "reply_to" => reply_to || run["reply_to"]
                }

              spawn(fn -> handle_analyze(replay) end)
              true

            _ ->
              publish_synapse_progress(%{
                "run_id" => run_id,
                "step" => "resume_failed",
                "tenant_id" => run["tenant_id"],
                "user_id" => run["user_id"],
                "error" => "run_not_resumable"
              })

              if reply_to do
                reply_nats_json(reply_to, Reply.error("run_not_resumable", :invalid_request))
              end

              false
          end

        if started? and reply_to do
          reply_nats_json(
            reply_to,
            Reply.ok(%{
              "run_id" => run_id,
              "status" => "resumed",
              "route" => route
            })
          )
        end
    end
  end

  def handle_feedback(payload) do
    session_id = Map.get(payload, "session_id", "unknown")
    summary = Map.get(payload, "summary", "")
    project_name = Map.get(payload, "project_name")
    cost_usd = Map.get(payload, "cost_usd")
    duration_ms = Map.get(payload, "duration_ms")

    cost_str = if cost_usd, do: " ($#{Float.round(cost_usd, 4)})", else: ""
    short = String.slice(summary, 0, 120)

    Logger.info("[Orchestrator] Recording feedback for session #{String.slice(session_id, 0, 8)}")

    # Phase 3: record to EventHistory (visible in future kickoffs via "history" source)
    BotArmySynapse.EventHistory.record_event(
      :claude_decision,
      "Session #{String.slice(session_id, 0, 8)}: #{short}#{cost_str}"
    )

    # Phase 3: persist to GTD log (survives restarts)
    dispatch_action(%{
      "type" => "gtd.log.create",
      "params" => %{
        "title" => "Claude session: #{String.slice(summary, 0, 80)}",
        "content" =>
          "session_id=#{session_id} duration=#{duration_ms}ms cost=#{cost_usd}\n\n#{summary}"
      }
    })

    # Phase 4: project-scoped entries when project_name is provided
    if project_name do
      BotArmySynapse.EventHistory.record_event(
        String.to_atom("claude_project_#{project_name}"),
        "#{short}#{cost_str}"
      )

      case BotArmySynapse.GoalStore.get_goal_by_name(project_name) do
        nil ->
          :ok

        goal ->
          BotArmySynapse.GoalStore.record_decision(goal["id"], "#{short}#{cost_str}")
      end

      dispatch_action(%{
        "type" => "gtd.log.create",
        "params" => %{
          "title" => "[#{project_name}] #{String.slice(summary, 0, 60)}",
          "content" => "project=#{project_name} session_id=#{session_id}\n\n#{summary}"
        }
      })
    end

    # Brain trigger: record decisions for all at-risk goals that prompted this session
    goal_names = Map.get(payload, "goal_names", []) || []

    for name <- goal_names do
      case BotArmySynapse.GoalStore.get_goal_by_name(name) do
        nil -> :ok
        goal -> BotArmySynapse.GoalStore.record_decision(goal["id"], "brain: #{short}#{cost_str}")
      end
    end
  end

  defp dispatch_action(%{"type" => type, "params" => params}) do
    case type do
      "pi_go.command.run" ->
        dispatch_pi_go_command(params)

      "pi-go.command.run" ->
        dispatch_pi_go_command(params)

      "gtd.task.create" ->
        bridge_payload =
          %{
            "title" => Map.get(params, "title", "Untitled"),
            "context" => Map.get(params, "context", "inbox"),
            "priority" => Map.get(params, "priority", "normal"),
            "description" => Map.get(params, "description"),
            "labels" => Map.get(params, "labels"),
            "project_id" => Map.get(params, "project_id"),
            "goal_id" => Map.get(params, "goal_id"),
            "parent_task_id" => Map.get(params, "parent_task_id")
          }
          |> Map.reject(fn {_k, v} -> is_nil(v) end)

        case request_nats("bridge.task.create", bridge_payload) do
          {:ok, body} ->
            parsed = parse_body(body)

            task_id =
              get_in(parsed, ["data", "task_id"]) || get_in(parsed, ["data", "task", "id"])

            title = get_in(parsed, ["data", "task", "title"]) || "Untitled"
            summary = "Task created: #{title}"

            response_text =
              if is_binary(task_id) and task_id != "" do
                "#{summary}\ntask_id: #{task_id}"
              else
                "#{summary}\n(Failed to get task_id)"
              end

            %{"type" => type, "status" => "ok", "data" => parsed, "completion" => response_text}

          {:error, r} ->
            %{"type" => type, "status" => "error", "error" => inspect(r)}
        end

      "gtd.log.create" ->
        envelope = %{
          "event" => "gtd.log.create",
          "event_id" => UUID.uuid4(),
          "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
          "source" => "bot_army_synapse",
          "source_node" => node() |> Atom.to_string(),
          "triggered_by" => "synapse_workflow",
          "schema_version" => "1.0",
          "payload" => %{
            "title" => Map.get(params, "title", ""),
            "content" => Map.get(params, "content", "")
          }
        }

        publish_nats("gtd.log.create", envelope)
        %{"type" => type, "status" => "ok", "data" => %{}}

      "discord.notify" ->
        payload = %{
          "text" => Map.get(params, "text", ""),
          "urgency" => Map.get(params, "urgency", "normal"),
          "source" => "synapse_workflow"
        }

        publish_nats("events.synapse.workflow.notification", payload)
        %{"type" => type, "status" => "ok", "data" => %{}}

      unknown ->
        %{"type" => unknown, "status" => "error", "error" => "unsupported_action_type"}
    end
  rescue
    e ->
      %{
        "type" => Map.get(%{"type" => type, "params" => params}, "type", "unknown"),
        "status" => "error",
        "error" => inspect(e)
      }
  end

  defp dispatch_pi_go_command(params) do
    command = Map.get(params, "command", "run")
    prompt = Map.get(params, "prompt", "")
    task_id = Map.get(params, "task_id")
    correlation_id = Map.get(params, "correlation_id", UUID.uuid4())
    tenant_id = Map.get(params, "tenant_id", BotArmyRuntime.Tenant.default_tenant_id())
    user_id = Map.get(params, "user_id")

    payload =
      %{
        "command" => command,
        "prompt" => prompt,
        "correlation_id" => correlation_id,
        "discord_channel_id" => Map.get(params, "discord_channel_id")
      }
      |> maybe_put_task_id(task_id)

    envelope =
      BotArmySynapse.build_envelope("pi-go.command.run", payload)
      |> maybe_put_user_id(user_id)
      |> Map.put("tenant_id", tenant_id)

    case publish_nats("pi-go.command.run", envelope) do
      :ok ->
        %{
          "type" => "pi_go.command.run",
          "status" => "accepted",
          "data" => %{
            "subject" => "pi-go.command.run",
            "command" => command,
            "task_id" => task_id,
            "correlation_id" => correlation_id
          }
        }

      {:error, reason} ->
        %{"type" => "pi_go.command.run", "status" => "error", "error" => inspect(reason)}
    end
  end

  defp maybe_put_user_id(envelope, nil), do: envelope
  defp maybe_put_user_id(envelope, ""), do: envelope

  defp maybe_put_user_id(envelope, user_id) do
    Map.put(envelope, "user_id", user_id)
  end

  defp maybe_put_task_id(payload, nil), do: payload
  defp maybe_put_task_id(payload, ""), do: payload
  defp maybe_put_task_id(payload, task_id), do: Map.put(payload, "task_id", task_id)

  defp maybe_put_project_id(payload, nil), do: payload
  defp maybe_put_project_id(payload, ""), do: payload
  defp maybe_put_project_id(payload, project_id), do: Map.put(payload, "project_id", project_id)

  defp request_nats(subject, payload), do: request_nats(subject, payload, 5_000)

  defp request_nats(subject, payload, timeout_ms)
       when is_integer(timeout_ms) and timeout_ms > 0 do
    with {:ok, conn} <-
           GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000),
         {:ok, json} <- Jason.encode(payload),
         {:ok, response} <-
           Gnat.request(conn, subject, json,
             receive_timeout: timeout_ms,
             headers: BotArmyRuntime.Tracing.inject_trace_context([])
           ) do
      {:ok, response.body}
    end
  end

  defp parse_body(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, map} -> map
      _ -> %{}
    end
  end

  defp parse_body(_), do: %{}

  @doc false
  def sanitize_discord_llm_response(response_text) when is_binary(response_text) do
    if likely_false_gtd_mutation_claim?(response_text) do
      """
      I haven't created or updated anything in GTD yet.
      Use `!create project <name>` / `!create task <title>` (or send a `Project:` + numbered task list) and I'll return actual IDs after creation.
      """
      |> String.trim()
    else
      response_text
    end
  end

  def sanitize_discord_llm_response(response_text), do: response_text

  defp likely_false_gtd_mutation_claim?(response_text) do
    text = String.downcase(response_text || "")
    mentions_gtd_entity? = String.contains?(text, ["gtd", "task", "tasks", "project", "projects"])

    claims_completed_action? =
      Regex.match?(
        ~r/\b(i|we)\s+(already\s+)?(created|added|set up|set this up|went ahead and created|finished|completed)\b/i,
        response_text
      ) or
        Regex.match?(~r/\b(it'?s|its)\s+done\b/i, response_text)

    mentions_gtd_entity? and claims_completed_action?
  end

  @doc false
  def build_daily_recommendation_for_scope(context_data, override_state, tenant_id, user_id) do
    build_daily_recommendation(context_data, override_state, tenant_id, user_id)
  end

  defp build_daily_recommendation(context_data, override_state, tenant_id, user_id) do
    gtd_tasks = Map.get(context_data, :gtd, [])
    calendar = Map.get(context_data, :calendar, [])
    fitness = Map.get(context_data, :fitness, %{})
    now = DateTime.utc_now()

    stale_sources = hydration_stale_sources(tenant_id)

    energy_level = infer_energy_level(fitness)
    candidates = build_action_candidates(gtd_tasks, calendar, energy_level, override_state)

    {mode, selected, alternatives} =
      case candidates do
        [] ->
          {"top3", default_break_action(), []}

        [first | rest] ->
          {"single", first, Enum.take(rest, 3)}
      end

    data_freshness = freshness_score(stale_sources)
    model_agreement = model_agreement_score(candidates)
    user_alignment = 0.5
    overall_confidence = 0.45 * data_freshness + 0.35 * model_agreement + 0.20 * user_alignment
    stale_count = length(stale_sources)

    final_mode =
      cond do
        stale_count >= 2 ->
          "top3"

        true ->
          "top3"

        overall_confidence < 0.55 ->
          "top3"

        true ->
          mode
      end

    %{
      "recommendation_id" => "dcl-rec-" <> UUID.uuid4(),
      "tenant_id" => tenant_id,
      "mode" => final_mode,
      "overall_confidence" => Float.round(overall_confidence, 2),
      "confidence_components" => %{
        "data_freshness_score" => Float.round(data_freshness, 2),
        "model_agreement_score" => Float.round(model_agreement, 2),
        "user_alignment_score" => Float.round(user_alignment, 2)
      },
      "recommended_action" => selected,
      "ranked_actions" => ranked_actions(candidates),
      "alternatives" => alternatives,
      "stale_sources" => stale_sources,
      "rationale" =>
        build_rationale(selected, stale_sources, energy_level, model_agreement, user_alignment),
      "generated_at" => DateTime.to_iso8601(now)
    }
  end

  defp build_action_candidates(gtd_tasks, calendar, energy_level, override_state) do
    next_event_minutes = next_event_minutes(calendar)

    gtd_candidates =
      gtd_tasks
      |> Enum.filter(&(Map.get(&1, "status", "active") in ["active", "inbox"]))
      |> Enum.reject(&(Map.get(&1, "id") in override_state.deferred_action_ids))
      |> Enum.map(fn task ->
        priority = Map.get(task, "priority", "normal")
        base = if priority == "high", do: 82.0, else: 70.0
        time_penalty = if next_event_minutes < 20, do: 10.0, else: 0.0
        energy_bonus = if energy_level == "high", do: 6.0, else: 0.0
        score = max(40.0, min(100.0, base - time_penalty + energy_bonus))

        %{
          "action_id" => Map.get(task, "id", UUID.uuid4()),
          "action_type" => "task",
          "title" => Map.get(task, "title", "Untitled task"),
          "score" => score,
          "estimated_minutes" => if(priority == "high", do: 45, else: 30),
          "why" => ["Task priority is #{priority}", "Calendar slack is #{next_event_minutes}m"]
        }
      end)

    candidates =
      if gtd_candidates == [] do
        [default_break_action()]
      else
        gtd_candidates
      end
      |> maybe_force_action(override_state.forced_action_id)
      |> Enum.sort_by(
        &{-Map.get(&1, "score", 0), Map.get(&1, "action_id", ""), Map.get(&1, "title", "")}
      )

    candidates
  end

  defp default_break_action do
    %{
      "action_id" => "break-5",
      "action_type" => "break",
      "title" => "Take a 5-minute reset and review inbox",
      "score" => 55.0,
      "estimated_minutes" => 5,
      "why" => ["No clear high-confidence candidate from current signals"]
    }
  end

  defp maybe_force_action(candidates, nil), do: candidates
  defp maybe_force_action(candidates, ""), do: candidates

  defp maybe_force_action(candidates, forced_action_id) do
    Enum.map(candidates, fn c ->
      if Map.get(c, "action_id") == forced_action_id do
        Map.put(c, "score", 100.0)
      else
        c
      end
    end)
  end

  defp infer_energy_level(fitness) when is_map(fitness) do
    cond do
      Map.get(fitness, "energy_level") in ["high", "medium", "low"] ->
        Map.get(fitness, "energy_level")

      Map.get(fitness, "readiness_score", 0.5) >= 0.75 ->
        "high"

      Map.get(fitness, "readiness_score", 0.5) >= 0.45 ->
        "medium"

      true ->
        "low"
    end
  end

  defp infer_energy_level(_), do: "medium"

  defp freshness_score([]), do: 0.95
  defp freshness_score(stale_sources), do: max(0.30, 1.0 - 0.18 * length(stale_sources))

  defp model_agreement_score([]), do: 0.55
  defp model_agreement_score([_single]), do: 0.80

  defp model_agreement_score(candidates) do
    sorted = Enum.sort_by(candidates, &Map.get(&1, "score", 0.0), :desc)
    top = sorted |> Enum.at(0) |> Map.get("score", 0.0)
    second = sorted |> Enum.at(1, %{"score" => top}) |> Map.get("score", top)
    margin = max(0.0, top - second)
    max(0.45, min(0.95, 0.55 + margin / 100.0))
  end

  defp next_event_minutes(calendar) when is_list(calendar) do
    case List.first(calendar) do
      %{"start_time" => start_time} when is_binary(start_time) ->
        case DateTime.from_iso8601(start_time) do
          {:ok, dt, _} ->
            max(0, div(DateTime.diff(dt, DateTime.utc_now(), :second), 60))

          _ ->
            120
        end

      _ ->
        120
    end
  end

  defp next_event_minutes(_), do: 120

  defp build_rationale(action, stale_sources, energy_level, model_agreement, user_alignment) do
    [
      "Energy fit is #{energy_level}",
      if(stale_sources == [],
        do: "All key sources are fresh",
        else: "Stale sources: #{Enum.join(stale_sources, ", ")}"
      ),
      "Model agreement is #{Float.round(model_agreement, 2)} and user alignment is #{Float.round(user_alignment, 2)}",
      "Selected action score is #{Float.round(Map.get(action, "score", 0.0), 1)}"
    ]
  end

  defp hydration_stale_sources(tenant_id) do
    case BotArmySynapse.ContextHydrationServer.stale_sources(tenant_id) do
      {:ok, stale_entries} when is_list(stale_entries) ->
        stale_entries
        |> Enum.map(&map_stale_source/1)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()

      _ ->
        []
    end
  end

  defp map_stale_source(%{"signal_class" => "task.signal.verification"}), do: "tasks"
  defp map_stale_source(%{"signal_class" => "context.state.current"}), do: "calendar"
  defp map_stale_source(%{"signal_class" => "system.health"}), do: "calendar"
  defp map_stale_source(%{"signal_class" => "system.capability.snapshot"}), do: "fitness"
  defp map_stale_source(%{"signal_class" => "system.risk.signal"}), do: "tasks"
  defp map_stale_source(%{"signal_class" => "ops.deploy.complete"}), do: "deployments"
  defp map_stale_source(%{"signal_class" => "ops.deploy.failed"}), do: "deployments"
  defp map_stale_source(%{"signal_class" => "ops.deploy.started"}), do: "deployments"
  defp map_stale_source(_), do: nil

  defp build_daily_command_status_payload(recommendation) do
    stale_sources = Map.get(recommendation, "stale_sources", [])
    confidence = Map.get(recommendation, "overall_confidence", 0.0)

    status =
      if stale_sources == [] and confidence >= 0.8 do
        "healthy"
      else
        "degraded"
      end

    %{
      "service" => "daily-command-layer",
      "status" => status,
      "uptime_seconds" => 0,
      "last_event_age_ms" => 0,
      "details" => %{
        "mode" => Map.get(recommendation, "mode"),
        "overall_confidence" => confidence,
        "stale_sources" => stale_sources
      }
    }
  end

  defp track_run_start(run_id, route, attrs) do
    BotArmySynapse.RunStore.put_run(run_id, %{
      "route" => route,
      "status" => "running",
      "created_at" => DateTime.utc_now() |> DateTime.to_iso8601()
    })

    BotArmySynapse.RunStore.mark_status(run_id, "running", attrs)
  end

  defp unwrap_payload(%{"payload" => inner}) when is_map(inner), do: inner
  defp unwrap_payload(payload) when is_map(payload), do: payload
  defp unwrap_payload(_), do: %{}

  defp extract_request_context(message, payload) do
    tenant_id =
      Map.get(payload || %{}, "tenant_id") ||
        Map.get(message || %{}, "tenant_id") ||
        BotArmyRuntime.Tenant.default_tenant_id()

    user_id =
      Map.get(payload || %{}, "user_id") ||
        Map.get(message || %{}, "user_id")

    run_id =
      Map.get(payload || %{}, "run_id") ||
        Map.get(message || %{}, "run_id") ||
        Map.get(message || %{}, "event_id") ||
        UUID.uuid4()

    %{tenant_id: tenant_id, user_id: user_id, run_id: run_id}
  end

  defp safe_daily_context do
    gather_context(["gtd", "calendar", "fitness"], "")
  rescue
    reason ->
      Logger.warning("Daily command fallback context used: #{inspect(reason)}")

      %{
        "gtd" => %{"error" => "context_unavailable"},
        "calendar" => %{"error" => "context_unavailable"},
        "fitness" => %{"error" => "context_unavailable"},
        "_meta" => %{"fallback" => true}
      }
  end

  defp normalize_tenant_id(nil), do: BotArmyRuntime.Tenant.default_tenant_id()
  defp normalize_tenant_id(""), do: BotArmyRuntime.Tenant.default_tenant_id()
  defp normalize_tenant_id(tenant_id), do: tenant_id

  @doc false
  def normalize_daily_override_payload(payload) when is_map(payload) do
    payload
    |> maybe_resolve_reorder_action_id()
    |> maybe_rewrite_reorder_decision()
  end

  def normalize_daily_override_payload(payload), do: payload

  defp valid_daily_override_payload?(payload) when is_map(payload) do
    case Map.get(payload, "decision") do
      "defer" ->
        present?(Map.get(payload, "replacement_action_id")) or
          present?(Map.get(payload, "action_id"))

      "replace" ->
        present?(Map.get(payload, "replacement_action_id")) or reorder_rank_resolvable?(payload)

      "reorder" ->
        present?(Map.get(payload, "replacement_action_id")) or reorder_rank_resolvable?(payload)

      decision when decision in @daily_override_decisions ->
        true

      _ ->
        false
    end
  end

  defp valid_daily_override_payload?(_), do: false

  defp ranked_actions(candidates) do
    candidates
    |> Enum.with_index(1)
    |> Enum.map(fn {action, idx} ->
      Map.put(action, "rank", idx)
    end)
    |> Enum.take(10)
  end

  defp maybe_resolve_reorder_action_id(payload) do
    if present?(Map.get(payload, "replacement_action_id")) do
      payload
    else
      case resolve_replacement_action_id(payload) do
        nil -> payload
        action_id -> Map.put(payload, "replacement_action_id", action_id)
      end
    end
  end

  defp maybe_rewrite_reorder_decision(%{"decision" => "reorder"} = payload),
    do: Map.put(payload, "decision", "replace")

  defp maybe_rewrite_reorder_decision(payload), do: payload

  defp resolve_replacement_action_id(payload) do
    with rank when is_integer(rank) and rank > 0 <- Map.get(payload, "replacement_rank"),
         ids when is_list(ids) <- Map.get(payload, "ranked_action_ids") do
      Enum.at(ids, rank - 1)
    else
      _ -> nil
    end
  end

  defp reorder_rank_resolvable?(payload) do
    present?(resolve_replacement_action_id(payload))
  end

  defp present?(value), do: is_binary(value) and value != ""

  @doc false
  def normalize_llm_request_type(nil), do: "chat"

  def normalize_llm_request_type(value) when is_binary(value) do
    normalized = value |> String.trim() |> String.downcase()
    if normalized in @llm_request_types, do: normalized, else: "chat"
  end

  def normalize_llm_request_type(_), do: "chat"

  @doc false
  def normalize_llm_model_preference(nil), do: "auto"

  def normalize_llm_model_preference(value) when is_binary(value) do
    normalized = value |> String.trim() |> String.downcase()
    if normalized in @llm_model_preferences, do: normalized, else: "auto"
  end

  def normalize_llm_model_preference(_), do: "auto"
end
