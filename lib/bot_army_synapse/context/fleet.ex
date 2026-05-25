defmodule BotArmySynapse.Context.Fleet do
  @moduledoc """
  Fleet health context handler.

  Reads bot heartbeat data from Health.Monitor ETS (in-process, zero latency)
  and classifies bots as online/stale/down for inclusion in LLM prompts.
  """

  require Logger

  @known_bots_fallback ~w(
    gtd
    llm
    terrain
    sre
    context_broker
    job_applications
    chore
    fitness
    claude_bridge
    synapse
    surface_discord
    database_backups
    learning
    advocacy
  )

  @stale_after_seconds 90

  @doc """
  Gather fleet health context from Registry heartbeats.

  Returns a map:
  %{
    "online" => ["gtd", "llm", ...],
    "stale"  => [{"chore", 312}, ...],
    "down"   => ["sre", ...],
    "online_count" => 8,
    "stale_count" => 2,
    "down_count" => 1
  }
  """
  def get_context do
    now = DateTime.utc_now()
    known = known_bots()

    bots = list_registry_bots()

    seen_ids =
      bots
      |> Enum.map(& &1["name"])
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    stale =
      bots
      |> Enum.map(&stale_entry(&1, now))
      |> Enum.reject(&is_nil/1)
      |> Enum.sort_by(fn {id, _age} -> id end)

    stale_ids = MapSet.new(Enum.map(stale, fn {id, _} -> id end))

    online =
      bots
      |> Enum.map(& &1["name"])
      |> Enum.reject(&is_nil/1)
      |> Enum.reject(&MapSet.member?(stale_ids, &1))
      |> Enum.sort()

    down =
      known
      |> Enum.filter(&(to_string(&1) not in seen_ids))
      |> Enum.sort()

    %{
      "online" => online,
      "stale" => stale,
      "down" => down,
      "online_count" => length(online),
      "stale_count" => length(stale),
      "down_count" => length(down)
    }
  end

  def handle_event(_message, event) do
    Logger.debug("Fleet event received: #{event}")
    :ok
  end

  defp list_registry_bots do
    case BotArmyRuntime.Registry.list_bots() do
      {:ok, bots} when is_list(bots) -> bots
      _ -> []
    end
  rescue
    _ -> []
  end

  defp stale_entry(%{"name" => name, "last_heartbeat" => last_heartbeat}, now)
       when is_binary(name) and is_binary(last_heartbeat) do
    case DateTime.from_iso8601(last_heartbeat) do
      {:ok, heartbeat_at, _offset} ->
        age_seconds = DateTime.diff(now, heartbeat_at, :second)
        if age_seconds > @stale_after_seconds, do: {name, age_seconds}, else: nil

      _ ->
        nil
    end
  end

  defp stale_entry(_bot, _now), do: nil

  defp known_bots(list_bots_fn \\ &BotArmyRuntime.Registry.list_bots/0) do
    case list_bots_fn.() do
      {:ok, bots} when is_list(bots) ->
        names =
          bots
          |> Enum.map(fn
            %{"name" => name} when is_binary(name) -> name
            %{name: name} when is_binary(name) -> name
            _ -> nil
          end)
          |> Enum.reject(&is_nil/1)
          |> Enum.uniq()

        if names == [], do: @known_bots_fallback, else: names

      _ ->
        @known_bots_fallback
    end
  rescue
    _ -> @known_bots_fallback
  end
end
