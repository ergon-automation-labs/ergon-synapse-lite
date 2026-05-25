defmodule BotArmySynapse.SynapseProgressHandler do
  @moduledoc """
  Relays Synapse run progress events to Discord threads when channel context exists.
  """

  require Logger

  def handle_event(message, _subject) when is_map(message) do
    payload = Map.get(message, "payload", %{})
    maybe_notify_discord(payload)
  end

  def handle_event(_, _), do: :ignored

  defp maybe_notify_discord(payload) when is_map(payload) do
    channel_id = Map.get(payload, "channel_id")

    if is_binary(channel_id) and channel_id != "" do
      run_id = Map.get(payload, "run_id", "unknown")
      step = Map.get(payload, "step", "working")
      error = Map.get(payload, "error")
      human = human_step(step)
      timing_suffix = format_timing_suffix(Map.get(payload, "timing"))

      suffix =
        if is_binary(error) and error != "" do
          "\nerror=#{String.slice(error, 0, 300)}"
        else
          ""
        end

      content =
        "synapse progress: #{human} (step=#{step}, run_id=#{run_id})#{timing_suffix}#{suffix}"

      publish_discord_relay(channel_id, content)
    else
      :ok
    end
  end

  defp human_step("preflight_started"), do: "starting preflight checks"
  defp human_step("context_lookup_started"), do: "gathering context from bots"
  defp human_step("context_lookup_completed"), do: "context gathering complete"
  defp human_step("llm_request_started"), do: "starting LLM reasoning"
  defp human_step("llm_request_dispatched"), do: "LLM request sent"
  defp human_step("finalizing_response"), do: "finalizing response"
  defp human_step("workflow_started"), do: "starting workflow actions"
  defp human_step("workflow_completed"), do: "workflow actions complete"
  defp human_step("completed"), do: "completed successfully"
  defp human_step("failed"), do: "failed"
  defp human_step(other) when is_binary(other), do: String.replace(other, "_", " ")
  defp human_step(_), do: "working"

  defp publish_discord_relay(channel_id, content) do
    subject = "discord.relay.#{channel_id}"

    body =
      Jason.encode!(%{
        "content" => content,
        "thread_id" => channel_id
      })

    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000) do
      {:ok, conn} ->
        Gnat.pub(conn, subject, body)

      {:error, reason} ->
        Logger.debug(
          "[SynapseProgressHandler] skipping discord relay channel=#{channel_id}: #{inspect(reason)}"
        )
    end
  end

  @doc false
  def format_timing_suffix(timing) when is_map(timing) do
    run_elapsed = Map.get(timing, "run_elapsed_ms")
    step_elapsed = Map.get(timing, "step_elapsed_ms")

    cond do
      is_integer(step_elapsed) and is_integer(run_elapsed) ->
        " (+#{step_elapsed}ms, total #{run_elapsed}ms)"

      is_integer(run_elapsed) ->
        " (total #{run_elapsed}ms)"

      true ->
        ""
    end
  end

  def format_timing_suffix(_), do: ""
end
