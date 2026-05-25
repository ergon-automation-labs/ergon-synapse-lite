defmodule BotArmySynapse.Context.State do
  @moduledoc """
  Context state handler for NATS service.

  Queries context broker for current context state.
  """

  require Logger

  def get_current do
    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      {:ok, conn} ->
        case Gnat.request(
               conn,
               "context.state.query",
               Jason.encode!(BotArmySynapse.build_envelope("context.state.query")),
               receive_timeout: 3000
             ) do
          {:ok, response} ->
            body = extract_body(response)

            case Jason.decode(body) do
              {:ok, data} -> BotArmySynapse.extract_payload(data)["state"] || %{}
              _ -> nil
            end

          {:error, reason} ->
            Logger.debug("Context state request failed: #{inspect(reason)}")
            nil
        end

      {:error, _reason} ->
        Logger.warning("NATS connection not available for context state")
        nil
    end
  end

  def handle_update(message) do
    payload = message["payload"]
    Logger.debug("Context state updated: #{inspect(payload)}")
    :ok
  end

  defp extract_body(%{body: body}), do: body
  defp extract_body(body) when is_binary(body), do: body
end
