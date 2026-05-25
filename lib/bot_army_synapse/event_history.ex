defmodule BotArmySynapse.EventHistory do
  @moduledoc """
  GenServer that accumulates fleet events in a rolling window.

  Stores last 200 events with a 24-hour TTL. Consumer pushes events here;
  the orchestrator includes recent events in LLM prompts.
  """

  use GenServer
  require Logger

  @max_events 200
  @ttl_minutes 24 * 60

  # API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def record_event(event_type, summary) do
    GenServer.cast(__MODULE__, {:record, event_type, summary})
  end

  def get_recent do
    GenServer.call(__MODULE__, {:get_recent, nil})
  end

  def get_recent(minutes) do
    GenServer.call(__MODULE__, {:get_recent, minutes})
  end

  def get_recent_by_type(event_type) do
    GenServer.call(__MODULE__, {:get_by_type, event_type})
  end

  # Callbacks

  @impl true
  def init(_opts) do
    # Schedule periodic eviction
    Process.send_after(self(), :evict, 60_000)
    {:ok, []}
  end

  @impl true
  def handle_cast({:record, event_type, summary}, events) do
    entry = %{
      timestamp: DateTime.utc_now(),
      event_type: event_type,
      summary: summary
    }

    events = [entry | events] |> Enum.take(@max_events)
    {:noreply, events}
  end

  @impl true
  def handle_call({:get_recent, nil}, _from, events) do
    {:reply, events, events}
  end

  def handle_call({:get_recent, minutes}, _from, events) do
    cutoff = DateTime.add(DateTime.utc_now(), -minutes * 60, :second)
    filtered = Enum.filter(events, &(DateTime.compare(&1.timestamp, cutoff) == :gt))
    {:reply, filtered, events}
  end

  def handle_call({:get_by_type, event_type}, _from, events) do
    filtered = Enum.filter(events, &(&1.event_type == event_type))
    {:reply, filtered, events}
  end

  @impl true
  def handle_info(:evict, events) do
    cutoff = DateTime.add(DateTime.utc_now(), -@ttl_minutes * 60, :second)
    events = Enum.filter(events, &(DateTime.compare(&1.timestamp, cutoff) == :gt))
    Process.send_after(self(), :evict, 60_000)
    {:noreply, events}
  end
end
