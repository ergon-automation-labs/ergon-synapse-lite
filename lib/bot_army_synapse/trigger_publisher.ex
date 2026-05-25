defmodule BotArmySynapse.TriggerPublisher do
  @moduledoc """
  Publishes `bot_army.claude.trigger.brain` envelopes to NATS.

  Called by DecisionEngine when a rule fires. Constructs a standard
  Bot Army envelope with the prompt, model, launcher, and context,
  then publishes to the `bot_army.claude.trigger.brain` subject.

  The Claude Bridge bot subscribes to `bot_army.claude.trigger.>`
  and will pick up these triggers to start a Claude Code session.
  """

  require Logger

  def publish(prompt, opts \\ %{}) do
    launcher = Map.get(opts, :launcher, "ollama")
    model = Map.get(opts, :model, "glm-5.1:cloud")
    max_turns = Map.get(opts, :max_turns, 10)
    max_budget_usd = Map.get(opts, :max_budget_usd, 1.0)
    context = Map.get(opts, :context, %{})

    goal_names = Map.get(context, :goal_names, [])

    envelope = %{
      "event_id" => UUID.uuid4(),
      "event" => "bot_army.claude.trigger.brain",
      "schema_version" => "1.0",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "source_node" => node() |> Atom.to_string(),
      "triggered_by" => "decision_engine",
      "payload" => %{
        "prompt" => prompt,
        "launcher" => launcher,
        "model" => model,
        "max_turns" => max_turns,
        "max_budget_usd" => max_budget_usd,
        "allowed_tools" => ["Read", "Glob", "Grep", "Bash"],
        "permission_mode" => "acceptEdits",
        "context" => context,
        "goal_names" => goal_names
      }
    }

    subject = "bot_army.claude.trigger.brain"

    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000),
         {:ok, json} <- Jason.encode(envelope) do
      :ok = Gnat.pub(conn, subject, json)

      Logger.info(
        "[TriggerPublisher] Published brain trigger: launcher=#{launcher}, model=#{model}, prompt=#{String.slice(prompt, 0, 60)}..."
      )

      :ok
    else
      {:error, reason} ->
        Logger.error("[TriggerPublisher] Failed to publish trigger: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
