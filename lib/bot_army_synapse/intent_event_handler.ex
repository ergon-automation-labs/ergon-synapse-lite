defmodule BotArmySynapse.IntentEventHandler do
  @moduledoc """
  Handles intent lifecycle events (proposed, vetoed, deferred, acted, aborted)
  for observability and decision-making context.

  Records intent activity in the knowledge graph and optionally forwards
  significant events (vetoed, aborted) to Discord via gossip protocol.
  """

  require Logger

  @doc """
  Handle an intent lifecycle event.
  """
  def handle(message, subject) do
    payload = message["payload"] || %{}
    event_name = extract_event_name(subject)

    Logger.info("[IntentEventHandler] Intent event: #{event_name}",
      bot_name: Map.get(payload, "bot_name"),
      action: Map.get(payload, "action"),
      event: event_name
    )

    case event_name do
      "proposed" ->
        record_intent_event(:proposed, payload)

      "vetoed" ->
        record_intent_event(:vetoed, payload)
        maybe_notify_vetoed(payload)

      "deferred" ->
        record_intent_event(:deferred, payload)

      "acted" ->
        record_intent_event(:acted, payload)

      "aborted" ->
        record_intent_event(:aborted, payload)
        maybe_notify_aborted(payload)

      _ ->
        Logger.debug("[IntentEventHandler] Unknown intent event: #{event_name}")
    end

    :ok
  end

  defp extract_event_name(subject) do
    subject
    |> String.split(".")
    |> List.last()
  end

  defp record_intent_event(event_type, payload) do
    summary = build_summary(event_type, payload)

    BotArmySynapse.EventHistory.record_event(event_type, summary)

    BotArmySynapse.Stores.KnowledgeStore.create_event(%{
      event_type: Atom.to_string(event_type),
      summary: summary,
      tenant_id: default_tenant_id(),
      metadata: payload,
      occurred_at: DateTime.utc_now()
    })
  end

  defp build_summary(:proposed, payload) do
    bot = Map.get(payload, "bot_name", "unknown")
    action = Map.get(payload, "action", "unknown")
    score = Map.get(payload, "score", 0)

    "Intent proposed: #{bot} #{action} (score=#{Float.round(score, 2)})"
  end

  defp build_summary(:vetoed, payload) do
    bot = Map.get(payload, "bot_name", "unknown")
    reason = Map.get(payload, "reason", "unknown")

    "Intent vetoed: #{bot} - #{reason}"
  end

  defp build_summary(:deferred, payload) do
    bot = Map.get(payload, "bot_name", "unknown")
    action = Map.get(payload, "action", "unknown")
    reason = Map.get(payload, "defer_reason", "unclear")

    "Intent deferred: #{bot} #{action} (#{reason})"
  end

  defp build_summary(:acted, payload) do
    bot = Map.get(payload, "bot_name", "unknown")
    action = Map.get(payload, "action", "unknown")

    "Intent acted: #{bot} #{action}"
  end

  defp build_summary(:aborted, payload) do
    bot = Map.get(payload, "bot_name", "unknown")
    action = Map.get(payload, "action", "unknown")

    "Intent aborted: #{bot} #{action}"
  end

  defp maybe_notify_vetoed(_payload) do
    # Future: forward to Discord via gossip channel
    # For now, just structured logging via record_intent_event
    :ok
  end

  defp maybe_notify_aborted(_payload) do
    # Future: forward to Discord via gossip channel
    # For now, just structured logging via record_intent_event
    :ok
  end

  defp default_tenant_id do
    Application.get_env(:bot_army_synapse, :default_tenant_id, "default")
  end
end
