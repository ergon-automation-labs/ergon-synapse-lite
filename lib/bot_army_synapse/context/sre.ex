defmodule BotArmySynapse.Context.SRE do
  @moduledoc """
  SRE context handler for NATS service.

  Queries SRE bot for active alerts and pod status.
  """

  require Logger

  def get_context do
    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      {:ok, conn} ->
        case Gnat.request(
               conn,
               "sre.alerts.active",
               Jason.encode!(BotArmySynapse.build_envelope("sre.alerts.active")),
               receive_timeout: 2000
             ) do
          {:ok, response} ->
            body = extract_body(response)

            case Jason.decode(body) do
              {:ok, data} -> BotArmySynapse.extract_payload(data)
              _ -> nil
            end

          {:error, reason} ->
            Logger.debug("SRE request failed: #{inspect(reason)}")
            nil
        end

      {:error, _reason} ->
        Logger.warning("NATS connection not available for SRE context")
        nil
    end
  end

  def handle_event(_message, event) do
    Logger.debug("SRE event received: #{event}")
    :ok
  end

  defp extract_body(%{body: body}), do: body
  defp extract_body(body) when is_binary(body), do: body
end
