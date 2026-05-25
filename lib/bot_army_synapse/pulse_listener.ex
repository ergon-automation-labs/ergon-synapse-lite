defmodule BotArmySynapse.PulseListener do
  @moduledoc """
  Listens to health pulses from all bots and aggregates fleet state.

  Subscribes to `bot.*.pulse` NATS subjects and maintains a cache of the latest
  pulse from each bot. This gives Synapse a real-time view of distributed health:
  - GTD: task state, goal stagnation signals
  - Claude Bridge: session activity, context sources used
  - Other bots: similar health indicators

  API:
  - `get_fleet_health/0` - Returns aggregated health across all bots
  - `get_bot_health/1` - Returns health for a specific bot

  Synapse uses pulses to:
  - Enrich goal context with latest task/session signals
  - Route queries intelligently ("this goal needs Claude's attention")
  - Correlate signals across bots (task stagnation + no recent decisions = at-risk)
  """

  use GenServer
  require Logger

  @reconnect_delay_ms 5_000
  # Pulses expire after 10 minutes
  @pulse_ttl_seconds 10 * 60
  @server __MODULE__

  # API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: @server)
  end

  @doc """
  Get aggregated fleet health from all known bots.

  Returns map of bot_name => health_data with timestamps.
  """
  def get_fleet_health do
    try do
      GenServer.call(@server, :get_fleet_health)
    catch
      :exit, _ -> %{}
    end
  end

  @doc """
  Get health for a specific bot (gtd, claude_bridge, etc).

  Returns health_data or nil if bot unknown.
  """
  def get_bot_health(bot_name) when is_binary(bot_name) do
    try do
      GenServer.call(@server, {:get_bot_health, bot_name})
    catch
      :exit, _ -> nil
    end
  end

  # Callbacks

  @impl true
  def init(_opts) do
    Logger.info("[PulseListener] Starting Synapse pulse listener (PID: #{inspect(self())})")

    state = %{
      subscriptions: [],
      pulses: %{},
      last_sync: DateTime.utc_now()
    }

    Logger.debug("[PulseListener] Init state: #{inspect(state)}")
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000) do
      {:ok, conn} ->
        BotArmyRuntime.NATS.Connection.subscribe_to_status()
        subscribe_to_pulses(conn, state)

      {:error, _reason} ->
        handle_connection_unavailable(state)
    end
  end

  defp subscribe_to_pulses(conn, state) do
    Logger.info("[PulseListener] Connected to NATS, subscribing to bot pulses")
    Logger.info("[PulseListener] Process PID: #{inspect(self())}")
    Logger.info("[PulseListener] Connection object: #{inspect(conn)}")

    case Gnat.sub(conn, self(), "bot.*.pulse") do
      {:ok, sub} ->
        Logger.info(
          "[PulseListener] Subscribed to bot.*.pulse with subscription: #{inspect(sub)}"
        )

        Logger.info(
          "[PulseListener] Subscription active, waiting for messages on topic: bot.*.pulse"
        )

        {:noreply, %{state | subscriptions: [sub]}}

      {:error, reason} ->
        Logger.error("[PulseListener] Failed to subscribe: #{inspect(reason)}")
        handle_connection_unavailable(state)
    end
  end

  defp handle_connection_unavailable(state) do
    Logger.warning("[PulseListener] NATS unavailable, retrying in #{@reconnect_delay_ms}ms")
    Process.send_after(self(), :reconnect, @reconnect_delay_ms)
    {:noreply, state}
  end

  @impl true
  def handle_info({:msg, msg}, state) do
    Logger.info("[PulseListener] handle_info matched {:msg, msg}: #{inspect(msg, limit: 100)}")
    handle_pulse_message(msg, state)
  end

  @impl true
  def handle_info({:nats, :disconnected}, state) do
    Logger.warning("[PulseListener] NATS connection lost")
    Process.send_after(self(), :reconnect, @reconnect_delay_ms)
    {:noreply, state}
  end

  @impl true
  def handle_info({:nats, :connected}, state) do
    Logger.info("[PulseListener] NATS connection restored")
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_info(:reconnect, state) do
    Logger.info("[PulseListener] handle_info matched :reconnect")
    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_info({_sid, :ok} = msg, state) do
    Logger.info("[PulseListener] handle_info matched subscription confirmation: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.info("[PulseListener] handle_info caught-all: #{inspect(msg, limit: 200)}")
    {:noreply, state}
  end

  defp handle_pulse_message(%{body: body}, state) do
    case Jason.decode(body) do
      {:ok, pulse} ->
        bot_name = Map.get(pulse, "bot", "unknown")

        Logger.debug(
          "[PulseListener] Received pulse from #{bot_name} at #{Map.get(pulse, "timestamp")}"
        )

        pulses = Map.put(state.pulses, bot_name, pulse)
        {:noreply, %{state | pulses: pulses, last_sync: DateTime.utc_now()}}

      {:error, reason} ->
        Logger.warning("[PulseListener] Failed to decode pulse: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  defp handle_pulse_message(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def handle_call(:get_fleet_health, _from, state) do
    # Filter out expired pulses
    now = DateTime.utc_now()

    active_pulses =
      state.pulses
      |> Enum.filter(fn {_bot, pulse} ->
        case DateTime.from_iso8601(Map.get(pulse, "timestamp", "")) do
          {:ok, ts, _} ->
            DateTime.diff(now, ts, :second) < @pulse_ttl_seconds

          :error ->
            false
        end
      end)
      |> Enum.into(%{})

    {:reply, active_pulses, %{state | pulses: active_pulses}}
  end

  @impl true
  def handle_call({:get_bot_health, bot_name}, _from, state) do
    pulse = Map.get(state.pulses, bot_name)

    reply =
      if pulse do
        case DateTime.from_iso8601(Map.get(pulse, "timestamp", "")) do
          {:ok, ts, _} ->
            if DateTime.diff(DateTime.utc_now(), ts, :second) < @pulse_ttl_seconds do
              pulse
            else
              nil
            end

          :error ->
            nil
        end
      else
        nil
      end

    {:reply, reply, state}
  end
end
