defmodule BotArmySynapse.Context.Calendar do
  @moduledoc """
  Calendar context handler for NATS service.

  Queries calendar bot for upcoming events.
  """

  require Logger

  def get_context do
    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      {:ok, conn} ->
        case Gnat.request(
               conn,
               "calendar.event.list",
               Jason.encode!(BotArmySynapse.build_envelope("calendar.event.list")),
               receive_timeout: 3000
             ) do
          {:ok, response} ->
            body = extract_body(response)

            with {:ok, data} <- Jason.decode(body) do
              events = Map.get(BotArmySynapse.extract_payload(data), "events", [])
              # Return upcoming events only
              events
              |> Enum.filter(&(&1["status"] != "cancelled"))
              |> Enum.take(10)
            else
              _ -> nil
            end

          {:error, reason} ->
            Logger.debug("Calendar request failed: #{inspect(reason)}")
            nil
        end

      {:error, _reason} ->
        Logger.warning("NATS connection not available for calendar context")
        nil
    end
  end

  def handle_event(_message, event) do
    Logger.debug("Calendar event received: #{event}")
    :ok
  end

  defp extract_body(%{body: body}), do: body
  defp extract_body(body) when is_binary(body), do: body
end
