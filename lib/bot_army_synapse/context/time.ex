defmodule BotArmySynapse.Context.Time do
  @moduledoc """
  Time context handler for NATS service.

  Provides time-based context for LLM queries.
  """

  def get_context do
    timezone = "America/Los_Angeles"
    now = DateTime.now!(timezone)

    %{
      "datetime" => DateTime.to_iso8601(now),
      "day_of_week" => DateTime.to_date(now) |> Date.day_of_week() |> day_name(),
      "hour" => now.hour,
      "timezone" => timezone
    }
  end

  defp day_name(1), do: "Monday"
  defp day_name(2), do: "Tuesday"
  defp day_name(3), do: "Wednesday"
  defp day_name(4), do: "Thursday"
  defp day_name(5), do: "Friday"
  defp day_name(6), do: "Saturday"
  defp day_name(7), do: "Sunday"
end
