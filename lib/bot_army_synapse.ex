defmodule BotArmySynapse do
  @moduledoc """
  Bot Army Synapse - RAG Orchestration and Decision Service.

  Acts as the central "synapse" for the Bot Army ecosystem:
  - Gathers context from multiple bots (GTD, Calendar, Context, etc.)
  - Formats prompts with rich context for LLM queries
  - Evaluates fleet events against rules to trigger Claude sessions
  - Routes LLM responses back to appropriate destinations

  ## Architecture

  The synapse is separate from `bot_army_llm` (the LLM proxy):
  - **Synapse** - orchestration, context gathering, decision engine
  - **LLM Proxy** - LLM API calls, provider routing, token accounting

  ## NATS Integration

  Subscribes to:
  - `llm.context.analyze` - Main orchestration entry point
  - `events.gtd.task.>` - Listen for task updates
  - `events.calendar.>` - Listen for calendar updates
  - `events.context.state.changed` - Context mode changes
  - `bot_army.claude.result.>` - Claude Bridge session results

  Publishes to:
  - `events.synapse.context.gathered` - Context gathering complete
  - `events.synapse.session.created` - New conversation session
  - `events.synapse.session.updated` - Session state change
  - `events.synapse.claude.result` - Claude result observability
  - `bot_army.claude.trigger.brain` - Trigger Claude sessions via DecisionEngine
  """

  @version "0.4.2"

  def version do
    @version
  end

  @doc """
  Build a Bot Army envelope for NATS requests.

  All bots use `Decoder.decode` which rejects messages without
  the required envelope fields. This wrapper ensures requests pass validation.

  Includes tenant_id at the top level (for handlers that read it from root)
  and in the payload (for handlers that read it from payload).
  """
  def build_envelope(event, payload \\ %{}) do
    tenant_id = BotArmyRuntime.Tenant.default_tenant_id()

    %{
      "event_id" => UUID.uuid4(),
      "event" => event,
      "tenant_id" => tenant_id,
      "schema_version" => "1.0",
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "source" => "bot_army_synapse",
      "source_node" => node() |> Atom.to_string(),
      "triggered_by" => "context_query",
      "payload" => Map.put_new(payload, "tenant_id", tenant_id)
    }
  end

  @doc """
  Extract payload from a Bot Army envelope response.

  If the response is wrapped in an envelope (has a "payload" key), unwrap it.
  Otherwise return the data as-is.
  """
  def extract_payload(data) when is_map(data) do
    Map.get(data, "payload", data)
  end

  def extract_payload(data), do: data
end
