defmodule BotArmySynapse.IntentEvaluatorPromptTest do
  use ExUnit.Case, async: true
  @moduletag :core

  alias BotArmySynapse.IntentEvaluator

  # `context.summary.<type>` is an aggregate produced by `AccumulatedContext.compute_summary/1`
  # (`%{count:, values:, latest: %{value:, ...}}`) — never a scalar. These tests carry the real
  # shape: a test that passes `%{}` is green for the right and the wrong read alike.

  describe "build_proactive_message_defer_prompt/3" do
    test "renders the newest pending count, not the aggregate map" do
      context = %{
        summary: %{
          pending_context_events: %{
            count: 2,
            values: [6, 9],
            latest: %{type: :pending_context_events, value: 9, metadata: %{source: "pulse"}}
          }
        }
      }

      prompt =
        IntentEvaluator.build_proactive_message_defer_prompt(
          "proactive_message",
          %{score: 0.3, reason: :low_roll},
          context
        )

      assert prompt["text"] =~ "There are 9 pending context events"
      refute prompt["text"] =~ "%{"
      refute prompt["text"] =~ "latest"
    end

    test "an absent observation type reads as 0" do
      prompt =
        IntentEvaluator.build_proactive_message_defer_prompt(
          "proactive_message",
          %{score: 0.3, reason: :low_roll},
          %{summary: %{user_idle_minutes: %{count: 1, values: [45], latest: %{value: 45}}}}
        )

      assert prompt["text"] =~ "There are 0 pending context events"
    end

    test "accepts a bare summary map as well as a snapshot" do
      prompt =
        IntentEvaluator.build_proactive_message_defer_prompt(
          "proactive_message",
          %{score: 0.3, reason: :low_roll},
          %{pending_context_events: %{count: 1, values: [12], latest: %{value: 12}}}
        )

      assert prompt["text"] =~ "There are 12 pending context events"
    end
  end
end
