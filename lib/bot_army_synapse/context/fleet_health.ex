defmodule BotArmySynapse.Context.FleetHealth do
  @moduledoc """
  Provides fleet health context from bot pulses.

  Reads aggregated health data from PulseListener and formats it for Claude
  to understand how the system is operating:
  - Which bots are active and healthy
  - What tasks/goals are being worked
  - Where bottlenecks or stagnation might be

  Queries ask: "How is the system doing? Which goals/bots need attention?"
  """

  require Logger

  @doc """
  Get current fleet health summary.

  Returns formatted context string suitable for inclusion in Claude's prompt.
  """
  def get_context do
    fleet = BotArmySynapse.PulseListener.get_fleet_health()

    if map_size(fleet) == 0 do
      nil
    else
      format_fleet_health(fleet)
    end
  end

  @doc """
  Handle GTD task updates - refresh pulse listener in case it has new data.

  This is called when gtd.task.updated events arrive, allowing us to
  correlate task changes with bot health.
  """
  def handle_event(_event, _message) do
    # Could trigger a refresh of goal health or task analysis here
    :ok
  end

  # Private

  defp format_fleet_health(fleet) do
    bot_summaries =
      fleet
      |> Enum.map(fn {bot_name, pulse} ->
        format_bot_pulse(bot_name, pulse)
      end)
      |> Enum.filter(&(not is_nil(&1)))
      |> Enum.join("\n")

    """
    ## Fleet Health Summary

    #{bot_summaries}

    _Aggregated from periodic bot pulses_
    """
    |> String.trim()
  end

  defp format_bot_pulse(bot_name, pulse) do
    observations = Map.get(pulse, "observations", %{})
    health_signal = Map.get(observations, "health_signal", "nominal")
    timestamp = Map.get(pulse, "timestamp", "unknown")

    case bot_name do
      "gtd" ->
        format_gtd_pulse(observations, health_signal, timestamp)

      "claude_bridge" ->
        format_claude_bridge_pulse(observations, health_signal, timestamp)

      _ ->
        format_generic_pulse(bot_name, observations, health_signal)
    end
  end

  defp format_gtd_pulse(observations, health_signal, _timestamp) do
    goals = Map.get(observations, "goals", %{})
    total_tasks = Map.get(observations, "total_active_tasks", 0)

    old_goal_count =
      goals
      |> Enum.count(fn {_id, goal_data} ->
        Map.get(goal_data, "tasks_older_than_7d", 0) > 0
      end)

    """
    **GTD** [#{health_signal}]: #{total_tasks} active tasks across #{map_size(goals)} goals
    - #{old_goal_count} goals have tasks stagnating 7+ days
    """
  end

  defp format_claude_bridge_pulse(_observations, health_signal, _timestamp) do
    """
    **Claude Bridge** [#{health_signal}]: Active and responding to requests
    """
  end

  defp format_generic_pulse(bot_name, _observations, health_signal) do
    """
    **#{String.capitalize(bot_name)}** [#{health_signal}]: Operational
    """
  end
end
