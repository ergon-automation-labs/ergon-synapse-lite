defmodule BotArmySynapse.Context.GTD do
  @moduledoc """
  GTD context handler for Synapse.

  Reads task state from GTD's cached pulse via PulseListener (no NATS request).
  This provides zero-latency fallback even if GTD is slow or unavailable.
  """

  require Logger

  def get_context do
    case BotArmySynapse.PulseListener.get_bot_health("gtd") do
      pulse when is_map(pulse) ->
        tasks = Map.get(pulse, "tasks", [])

        tasks
        |> filter_incomplete_tasks()
        |> fallback_to_live_tasks()

      nil ->
        Logger.debug("GTD pulse not available, falling back to live task query")
        fetch_live_tasks()
    end
  end

  def handle_event(_message, event) do
    Logger.debug("GTD event received: #{event}")
    :ok
  end

  defp filter_incomplete_tasks(tasks) when is_list(tasks) do
    tasks
    |> Enum.filter(&(&1["status"] not in ["completed", "deleted"]))
    |> Enum.take(10)
  end

  defp filter_incomplete_tasks(_), do: []

  defp fallback_to_live_tasks([]), do: fetch_live_tasks()
  defp fallback_to_live_tasks(tasks), do: tasks

  defp fetch_live_tasks do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000),
         {:ok, response} <-
           Gnat.request(
             conn,
             "gtd.task.list",
             Jason.encode!(BotArmySynapse.build_envelope("gtd.task.list", %{"limit" => 10})),
             receive_timeout: 2_000
           ),
         {:ok, decoded} <- Jason.decode(extract_body(response)) do
      decoded
      |> BotArmySynapse.extract_payload()
      |> Map.get("tasks", [])
      |> filter_incomplete_tasks()
    else
      _ ->
        nil
    end
  end

  defp extract_body(%{body: body}), do: body
  defp extract_body(body) when is_binary(body), do: body
end
