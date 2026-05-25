defmodule BotArmySynapse.NATS.LogsResponder do
  @moduledoc """
  Bridge API responder for event log queries.

  Subscribes to:
  - `bridge.logs.query` — search synapse event log by bot name or text

  Responds with standard bridge reply format:
  ```json
  {
    "ok": true,
    "data": { "events": [...], "count": N },
    "schema_version": "1.0",
    "timestamp": "..."
  }
  ```
  """

  use GenServer
  require Logger

  alias BotArmyRuntime.NATS.Reply
  alias BotArmySynapse.Stores.KnowledgeStore

  @reconnect_delay_ms 5000
  @version Mix.Project.config()[:version]

  @subjects [
    %{subject: "bridge.logs.query", type: :request_reply, description: "Query event logs"}
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    Logger.info("[LogsResponder] Starting logs responder")
    state = %{subscriptions: [], conn: nil, opts: opts}
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      {:ok, conn} ->
        BotArmyRuntime.NATS.Connection.subscribe_to_status()
        Logger.info("[LogsResponder] Connected to NATS, subscribing to log topics")

        subscriptions = setup_subscriptions(conn)
        BotArmyRuntime.Registry.register("logs_responder", @subjects, @version)

        {:noreply, %{state | subscriptions: subscriptions, conn: conn}}

      {:error, _reason} ->
        Logger.warning("[LogsResponder] NATS connection not ready, will retry")
        Process.send_after(self(), :connect_retry, @reconnect_delay_ms)
        {:noreply, state}
    end
  end

  defp setup_subscriptions(conn) do
    subjects = ["bridge.logs.query"]

    subjects
    |> Enum.map(fn subject ->
      case Gnat.sub(conn, self(), subject) do
        {:ok, sub} ->
          Logger.info("[LogsResponder] Subscribed to #{subject}")
          sub

        {:error, reason} ->
          Logger.error("[LogsResponder] Failed to subscribe to #{subject}: #{inspect(reason)}")
          nil
      end
    end)
    |> Enum.filter(&(not is_nil(&1)))
  end

  @impl true
  def handle_info(:connect_retry, state) do
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info({:msg, msg}, state) do
    BotArmyRuntime.Tracing.with_consumer_span(msg.topic, Map.get(msg, :headers), fn ->
      Logger.debug("[LogsResponder] Received NATS message on subject: #{msg.topic}")

      case msg.topic do
        "bridge.logs.query" -> handle_logs_query(msg)
        _ -> :ok
      end
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:nats, :disconnected}, state) do
    Logger.warning("[LogsResponder] Disconnected from NATS, will reconnect")
    Process.send_after(self(), :connect_retry, @reconnect_delay_ms)
    {:noreply, %{state | subscriptions: [], conn: nil}}
  end

  @impl true
  def handle_info({:nats, :connected}, state) do
    Logger.info("[LogsResponder] Reconnected to NATS, re-subscribing")
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info(:reconnect, state) do
    {:noreply, state, {:continue, :connect}}
  end

  defp handle_logs_query(msg) do
    params =
      case decode_json(msg.body) do
        {:ok, p} -> p
        _ -> %{}
      end

    query = Map.get(params, "query", "")
    limit = min(Map.get(params, "limit", 100), 500)
    since = Map.get(params, "since")

    since_dt =
      case since do
        nil ->
          nil

        str when is_binary(str) ->
          case DateTime.from_iso8601(str) do
            {:ok, dt, _} -> dt
            _ -> nil
          end

        _ ->
          nil
      end

    opts = [limit: limit]
    opts = if since_dt, do: Keyword.put(opts, :from, since_dt), else: opts

    case KnowledgeStore.query(default_tenant_id(), query, opts) do
      {:ok, result} ->
        events = serialize_events(result.events)
        reply(msg, Reply.ok(%{"events" => events, "count" => length(events)}))

      {:error, reason} ->
        Logger.warning("[LogsResponder] Failed to query logs: #{inspect(reason)}")
        reply(msg, Reply.error(inspect(reason), :database_error))
    end
  end

  defp reply(%{reply_to: nil}, _body), do: :ok

  defp reply(%{reply_to: reply_to}, body) do
    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5000) do
      headers = BotArmyRuntime.Tracing.inject_trace_context([])

      payload =
        cond do
          is_binary(body) -> body
          is_map(body) -> Jason.encode!(body)
          true -> to_string(body)
        end

      Gnat.pub(conn, reply_to, payload, headers: headers)
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

  defp default_tenant_id,
    do: System.get_env("BOT_ARMY_TENANT_ID", "00000000-0000-0000-0000-000000000001")

  defp decode_json(body) do
    Jason.decode(body)
  end
end
