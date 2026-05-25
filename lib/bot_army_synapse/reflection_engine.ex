defmodule BotArmySynapse.ReflectionEngine do
  @moduledoc """
  Reflection loop for Synapse orchestration quality.

  Tracks predicted context sources per session, compares them with actual
  outcome feedback from surfaces, and publishes mismatch events so Synapse
  can learn which sources are worth gathering.

  Also provides adaptive source pruning based on historical OutcomeFeedbackStore
  statistics.
  """

  use GenServer
  require Logger

  @prediction_ttl_ms 86_400_000
  @helpful_rate_threshold 0.3
  @latency_threshold_ms 5_000
  @min_samples 5
  @obs_source_pruned "events.synapse.reflection.source_pruned"

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Record a prediction of which context sources Synapse expects to need.

  `run_id` should match the run_id used in progress events and feedback.
  """
  def record_prediction(run_id, predicted_sources, question) do
    GenServer.cast(__MODULE__, {:record_prediction, run_id, predicted_sources, question})
  end

  @doc """
  Match an incoming outcome feedback event against the stored prediction.

  Computes missed sources (predicted but not in feedback's used list) and
  false-positive sources (predicted and present but marked unhelpful).

  Publishes `bot_army.synapse.reflection.mismatch` when mismatches exist.
  """
  def match_feedback(feedback) do
    GenServer.cast(__MODULE__, {:match_feedback, feedback})
  end

  @doc """
  Filter a list of context sources by historical quality.

  Sources with `helpful_rate < 0.3` and at least 5 samples are dropped.
  Sources with `avg_latency_ms > 5000` and at least 5 samples are dropped.
  """
  def prune_sources(sources, opts \\ []) do
    GenServer.call(__MODULE__, {:prune_sources, sources, opts})
  end

  @doc """
  Get mismatches for a specific run (for debugging / testing).
  """
  def get_mismatches(run_id) do
    GenServer.call(__MODULE__, {:get_mismatches, run_id})
  end

  @doc """
  Reset internal state (for testing).
  """
  def reset do
    GenServer.cast(__MODULE__, :reset)
  end

  # GenServer callbacks

  @impl true
  def init(_opts) do
    schedule_cleanup()
    {:ok, %{predictions: %{}, mismatches: %{}}}
  end

  @impl true
  def handle_cast({:record_prediction, run_id, predicted_sources, question}, state) do
    prediction = %{
      predicted_sources: predicted_sources,
      question: question,
      recorded_at: System.monotonic_time(:millisecond)
    }

    predictions = Map.put(state.predictions, run_id, prediction)
    {:noreply, %{state | predictions: predictions}}
  end

  @impl true
  def handle_cast({:match_feedback, feedback}, state) do
    run_id = Map.get(feedback, "run_id") || Map.get(feedback, "task_id", "unknown")
    actual_sources = Map.get(feedback, "context_sources_used", [])
    was_helpful = Map.get(feedback, "was_helpful")

    case Map.pop(state.predictions, run_id) do
      {nil, _predictions} ->
        {:noreply, state}

      {prediction, predictions} ->
        predicted = prediction.predicted_sources || []

        missed =
          predicted
          |> Enum.reject(fn s -> s in actual_sources end)

        false_positives =
          if was_helpful == false do
            predicted |> Enum.filter(fn s -> s in actual_sources end)
          else
            []
          end

        mismatches = %{
          run_id: run_id,
          question: prediction.question,
          predicted: predicted,
          actual: actual_sources,
          missed: missed,
          false_positives: false_positives,
          was_helpful: was_helpful,
          detected_at: DateTime.utc_now() |> DateTime.to_iso8601()
        }

        if missed != [] or false_positives != [] do
          publish_mismatch(mismatches)
          _ = create_mismatch_task(mismatches)
        end

        new_mismatches = Map.put(state.mismatches, run_id, mismatches)
        {:noreply, %{state | predictions: predictions, mismatches: new_mismatches}}
    end
  end

  @impl true
  def handle_cast(:reset, _state) do
    {:noreply, %{predictions: %{}, mismatches: %{}}}
  end

  @impl true
  def handle_call({:prune_sources, sources, opts}, _from, state) do
    threshold = Keyword.get(opts, :helpful_rate_threshold, @helpful_rate_threshold)
    latency_threshold = Keyword.get(opts, :latency_threshold_ms, @latency_threshold_ms)
    min_samples = Keyword.get(opts, :min_samples, @min_samples)

    pruned =
      Enum.filter(sources, fn source ->
        case BotArmySynapse.OutcomeFeedbackStore.stats(source) do
          nil ->
            true

          stats ->
            keep_helpful =
              stats.total_count < min_samples or stats.helpful_rate >= threshold

            keep_latency =
              stats.total_count < min_samples or
                is_nil(stats.avg_latency_ms) or
                stats.avg_latency_ms <= latency_threshold

            keep = keep_helpful and keep_latency

            unless keep do
              reason =
                if not keep_helpful,
                  do: "helpful_rate=#{stats.helpful_rate} < #{threshold}",
                  else: "latency=#{stats.avg_latency_ms}ms > #{latency_threshold}ms"

              Logger.info(
                "[ReflectionEngine] Pruning source=#{source} (#{reason}, n=#{stats.total_count})"
              )

              _ = emit_source_pruned(source, stats)
            end

            keep
        end
      end)

    {:reply, pruned, state}
  end

  @impl true
  def handle_call({:get_mismatches, run_id}, _from, state) do
    {:reply, Map.get(state.mismatches, run_id), state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    now = System.monotonic_time(:millisecond)

    predictions =
      Enum.reject(state.predictions, fn {_run_id, pred} ->
        age = now - pred.recorded_at
        age > @prediction_ttl_ms
      end)
      |> Map.new()

    schedule_cleanup()
    {:noreply, %{state | predictions: predictions}}
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @prediction_ttl_ms)
  end

  defp publish_mismatch(mismatches) do
    event = %{
      "event_id" => UUID.uuid4(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "schema_version" => "1.0",
      "event" => "synapse.reflection.mismatch",
      "tenant_id" => BotArmyRuntime.Tenant.default_tenant_id(),
      "payload" => %{
        "run_id" => mismatches.run_id,
        "question" => mismatches.question,
        "predicted_sources" => mismatches.predicted,
        "actual_sources" => mismatches.actual,
        "missed_sources" => mismatches.missed,
        "false_positive_sources" => mismatches.false_positives,
        "was_helpful" => mismatches.was_helpful
      }
    }

    try do
      case BotArmyCore.NATS.publish("bot_army.synapse.reflection.mismatch", event) do
        :ok ->
          log_mismatch_published(mismatches)

        {:ok, _subject} ->
          log_mismatch_published(mismatches)

        {:error, reason} ->
          Logger.warning("[ReflectionEngine] Failed to publish mismatch: #{inspect(reason)}")
      end
    catch
      :exit, reason ->
        Logger.warning(
          "[ReflectionEngine] NATS unavailable, mismatch logged locally: #{inspect(reason)}"
        )
    end
  end

  defp log_mismatch_published(mismatches) do
    Logger.info(
      "[ReflectionEngine] Published mismatch for run=#{mismatches.run_id} missed=#{length(mismatches.missed)} fp=#{length(mismatches.false_positives)}"
    )
  end

  defp create_mismatch_task(mismatches) do
    missed_str = Enum.join(mismatches.missed, ", ")
    fp_str = Enum.join(mismatches.false_positives, ", ")

    envelope = %{
      "event" => "gtd.task.create",
      "event_id" => Ecto.UUID.generate(),
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "tenant_id" => BotArmyRuntime.Tenant.default_tenant_id(),
      "user_id" => "system",
      "payload" => %{
        "title" => "Synapse reflection mismatch detected",
        "description" =>
          "Run: #{mismatches.run_id}\n" <>
            "Question: #{mismatches.question}\n\n" <>
            "Missed sources (predicted but not used): #{if missed_str != "", do: missed_str, else: "(none)"}\n\n" <>
            "False positives (predicted but unhelpful): #{if fp_str != "", do: fp_str, else: "(none)"}",
        "labels" => ["factory:proposal", "reflection:mismatch"],
        "priority" => "medium"
      }
    }

    try do
      case BotArmyRuntime.NATS.Publisher.publish("gtd.task.create", envelope) do
        {:ok, _} ->
          Logger.debug(
            "[ReflectionEngine] Created GTD task for mismatch run=#{mismatches.run_id}"
          )

        {:error, reason} ->
          Logger.warning("[ReflectionEngine] Failed to create mismatch task: #{inspect(reason)}")
      end
    catch
      :exit, reason ->
        Logger.warning(
          "[ReflectionEngine] GTD task creation failed (NATS unavailable): #{inspect(reason)}"
        )
    end
  end

  defp emit_source_pruned(source, stats) do
    payload = %{
      "source" => source,
      "helpful_rate" => stats.helpful_rate,
      "avg_latency_ms" => stats.avg_latency_ms,
      "total_count" => stats.total_count
    }

    BotArmyRuntime.NATS.Publisher.publish(@obs_source_pruned, payload)
    |> case do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, _reason} -> :ok
    end
  end
end
