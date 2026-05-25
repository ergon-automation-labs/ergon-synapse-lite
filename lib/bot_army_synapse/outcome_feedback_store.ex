defmodule BotArmySynapse.OutcomeFeedbackStore do
  @moduledoc """
  In-memory store for outcome feedback events.

  Surfaces publish `bot_army.outcome.feedback` when a user acts on or ignores
  bot output. This store aggregates per-source statistics for context ROI
  tracking and adaptive orchestration.

  Statistics tracked per source:
  - helpful_count / total_count → accuracy ratio
  - edited_count → how often users had to fix the output
  - avg_latency_ms → time-to-response
  - last_seen_at → recency for stale-source detection
  """

  use GenServer
  require Logger

  @default_max_events 1000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def record(feedback) do
    GenServer.cast(__MODULE__, {:record, feedback})
  end

  def stats(source) do
    GenServer.call(__MODULE__, {:stats, source})
  end

  def all_stats do
    GenServer.call(__MODULE__, :all_stats)
  end

  def sources do
    GenServer.call(__MODULE__, :sources)
  end

  def recent(limit \\ 50) do
    GenServer.call(__MODULE__, {:recent, limit})
  end

  def reset do
    GenServer.cast(__MODULE__, :reset)
  end

  @impl true
  def init(_opts) do
    {:ok, %{sources: %{}, events: []}}
  end

  @impl true
  def handle_cast({:record, feedback}, state) do
    source = Map.get(feedback, "source", "unknown")
    was_helpful = Map.get(feedback, "was_helpful")
    edited = Map.get(feedback, "edited_by_user", false)
    latency = Map.get(feedback, "latency_ms")
    now = DateTime.utc_now()

    current = Map.get(state.sources, source, %{})

    updated =
      current
      |> Map.update(:total_count, 1, &(&1 + 1))
      |> Map.update(:helpful_count, if(was_helpful == true, do: 1, else: 0), fn c ->
        if was_helpful == true, do: c + 1, else: c
      end)
      |> Map.update(:edited_count, if(edited, do: 1, else: 0), fn c ->
        if edited, do: c + 1, else: c
      end)
      |> Map.update(:latency_sum_ms, if(is_integer(latency), do: latency, else: 0), fn s ->
        if is_integer(latency), do: s + latency, else: s
      end)
      |> Map.update(:latency_count, if(is_integer(latency), do: 1, else: 0), fn c ->
        if is_integer(latency), do: c + 1, else: c
      end)
      |> Map.put(:last_seen_at, now)

    sources = Map.put(state.sources, source, updated)
    events = [feedback | state.events] |> Enum.take(@default_max_events)

    Logger.info(
      "[OutcomeFeedbackStore] Recorded feedback for source=#{source} helpful=#{was_helpful}"
    )

    {:noreply, %{state | sources: sources, events: events}}
  end

  @impl true
  def handle_cast(:reset, _state) do
    {:noreply, %{sources: %{}, events: []}}
  end

  @impl true
  def handle_call({:stats, source}, _from, state) do
    stats =
      case Map.get(state.sources, source) do
        nil ->
          nil

        data ->
          helpful_rate =
            if data.total_count > 0,
              do: Float.round(data.helpful_count / data.total_count, 2),
              else: 0.0

          avg_latency =
            if data.latency_count > 0,
              do: Float.round(data.latency_sum_ms / data.latency_count, 1),
              else: nil

          %{
            source: source,
            total_count: data.total_count,
            helpful_count: data.helpful_count,
            helpful_rate: helpful_rate,
            edited_count: data.edited_count,
            avg_latency_ms: avg_latency,
            last_seen_at: data.last_seen_at
          }
      end

    {:reply, stats, state}
  end

  @impl true
  def handle_call(:all_stats, _from, state) do
    stats =
      Enum.map(state.sources, fn {source, data} ->
        helpful_rate =
          if data.total_count > 0,
            do: Float.round(data.helpful_count / data.total_count, 2),
            else: 0.0

        avg_latency =
          if data.latency_count > 0,
            do: Float.round(data.latency_sum_ms / data.latency_count, 1),
            else: nil

        %{
          source: source,
          total_count: data.total_count,
          helpful_count: data.helpful_count,
          helpful_rate: helpful_rate,
          edited_count: data.edited_count,
          avg_latency_ms: avg_latency,
          last_seen_at: data.last_seen_at
        }
      end)

    {:reply, stats, state}
  end

  @impl true
  def handle_call(:sources, _from, state) do
    {:reply, Map.keys(state.sources), state}
  end

  @impl true
  def handle_call({:recent, limit}, _from, state) do
    {:reply, Enum.take(state.events, limit), state}
  end
end
