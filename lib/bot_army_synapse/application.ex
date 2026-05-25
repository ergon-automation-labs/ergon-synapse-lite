defmodule BotArmySynapse.Application do
  @moduledoc """
  Synapse service application supervisor.

  Manages RAG orchestration, decision engine, and knowledge graph
  across the Bot Army ecosystem.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Database repository
      BotArmySynapse.Repo,
      # Decision engine - evaluates fleet events against rules
      {BotArmySynapse.DecisionEngine, []},
      # Event history buffer - rolling window of fleet events
      {BotArmySynapse.EventHistory, []},
      # Conversation memory - Q&A pairs per session
      {BotArmySynapse.ConversationStore, []},
      # Run memory - resumable run context keyed by run_id
      {BotArmySynapse.RunStore, []},
      # Run retention - prune old persisted run records
      {BotArmySynapse.RunRetentionScheduler, []},
      # Goals cache - GTD projects enriched with synapse metadata
      {BotArmySynapse.GoalStore, []},
      # Pulse listener - aggregates health from all bots
      {BotArmySynapse.PulseListener, []},
      # Event-driven hydration cache for fast cross-bot answers
      {BotArmySynapse.ContextHydrationServer, []},
      # Gossip resolver for topic-agnostic cross-bot coordination
      {BotArmySynapse.GossipCoordinator, []},
      # Proactive gossip — posts to Discord when LLM has something worth sharing
      {BotArmySynapse.GossipScheduler, []},
      # Outcome feedback store — per-source context ROI tracking
      {BotArmySynapse.OutcomeFeedbackStore, []},
      # Reflection engine — compares predicted vs actual context needs
      {BotArmySynapse.ReflectionEngine, []},
      # Per-user timeline memory for cross-bot recall
      {BotArmySynapse.MemoryBroker, []},
      # Fleet liveness signal for Synapse itself
      {BotArmySynapse.PulsePublisher, []},
      # Intent evaluator — proactive messages when context warrants it
      {BotArmySynapse.IntentEvaluator, []},
      # Logs responder — bridge API for event log queries
      {BotArmySynapse.NATS.LogsResponder, []},
      # NATS consumer for synapse orchestration
      {BotArmySynapse.NATS.Consumer, []}
    ]

    opts = [strategy: :one_for_one, name: BotArmySynapse.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
