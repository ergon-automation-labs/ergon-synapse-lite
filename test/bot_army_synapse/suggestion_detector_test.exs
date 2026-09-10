defmodule BotArmySynapse.SuggestionDetectorTest do
  use ExUnit.Case, async: true

  alias BotArmySynapse.SuggestionDetector

  @now DateTime.utc_now()
  defp days_ago(n), do: DateTime.add(@now, -n * 86_400, :second) |> DateTime.to_iso8601()
  defp hours_ago(h), do: DateTime.add(@now, -h * 3600, :second) |> DateTime.to_iso8601()

  describe "classify_tasks/1" do
    test "buckets tasks by status and flags overdue/stale" do
      overdue = %{"id" => 1, "status" => "active", "due_date" => "2020-01-01",
                  "updated_at" => days_ago(30), "created_at" => days_ago(30)}
      stale = %{"id" => 2, "status" => "next_action",
                "updated_at" => days_ago(30), "created_at" => days_ago(30)}
      fresh = %{"id" => 3, "status" => "next_action",
                "updated_at" => days_ago(1), "created_at" => days_ago(1)}
      done = %{"id" => 4, "status" => "completed"}

      result = SuggestionDetector.classify_tasks([overdue, stale, fresh, done])

      assert [^overdue] = result[:overdue]
      assert [^stale, ^fresh] = result[:next_action] |> Enum.sort_by(& &1["id"])
      assert [^overdue, ^stale] = result[:stale] |> Enum.sort_by(& &1["id"])
      refute Map.get(result, :completed)
    end

    test "completed/deleted tasks are rejected entirely" do
      result = SuggestionDetector.classify_tasks([%{"status" => "completed"}])
      assert result[:active] == []
      assert result[:stale] == []
    end

    test "nil input yields an empty map" do
      assert SuggestionDetector.classify_tasks(nil) == %{}
    end
  end

  describe "stale_goals/2" do
    test "flags goals with no decision or older than the threshold" do
      fresh = %{"name" => "fresh", :last_decision_at => DateTime.add(@now, -1, :day),
                "updated_at" => days_ago(1)}
      old = %{"name" => "old", :last_decision_at => DateTime.add(@now, -30, :day),
              "updated_at" => days_ago(2)}
      never = %{"name" => "never", "updated_at" => days_ago(3)}

      result = SuggestionDetector.stale_goals([fresh, old, never], 7)

      names = Enum.map(result, & &1["name"])
      assert "old" in names and "never" in names
      refute "fresh" in names
    end

    test "nil input yields an empty list" do
      assert SuggestionDetector.stale_goals(nil, 7) == []
    end
  end

  describe "stale_tasks/3" do
    test "splits inbox by hours and active by days" do
      old_inbox = %{"status" => "inbox", "created_at" => hours_ago(100), "updated_at" => hours_ago(100)}
      new_inbox = %{"status" => "inbox", "created_at" => hours_ago(1), "updated_at" => hours_ago(1)}
      stale_active = %{"status" => "active", "created_at" => days_ago(20), "updated_at" => days_ago(20)}

      result = SuggestionDetector.stale_tasks([old_inbox, new_inbox, stale_active], 7, 48)

      assert result.inbox == [old_inbox]
      assert result.active == [stale_active]
    end

    test "nil input yields empty buckets" do
      assert SuggestionDetector.stale_tasks(nil, 7, 48) == %{inbox: [], active: []}
    end
  end

  describe "system_health_issues/1" do
    test "summarizes degraded bots and ignores nominal recent-pulse bots" do
      degraded = %{name: "a", health: "failing", last_pulse_at: nil}
      healthy = %{name: "b", health: "nominal", last_pulse_at: @now}

      [summary] = SuggestionDetector.system_health_issues([healthy, degraded])

      assert summary.bot_name == "a"
      assert summary.status == "failing"
      assert summary.description =~ "a: failing"
    end

    test "nominal bot with stale pulse counts as degraded" do
      stale_pulse = %{name: "c", health: "nominal", last_pulse_at: DateTime.add(@now, -3600, :second)}
      [summary] = SuggestionDetector.system_health_issues([stale_pulse])
      assert summary.bot_name == "c"
      assert summary.description =~ "pulse"
    end

    test "nil input yields an empty list" do
      assert SuggestionDetector.system_health_issues(nil) == []
    end
  end
end
