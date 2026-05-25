import Config

# Ecto repositories
config :bot_army_synapse, ecto_repos: [BotArmySynapse.Repo]

# Intent thresholds for synapse heartbeat decisions
config :bot_army_synapse, :intent_thresholds, %{
  pending_context_events: %{min: 5, weight: 0.5},
  user_idle_minutes: %{min: 30, weight: 0.3},
  relevant_insight: %{min: 1, weight: 0.2},
  random_threshold: 0.6
}

# Context sources - can be overridden by environment variable or Salt pillar
# Environment: BOT_ARMY_SYNAPSE_CONTEXT_SOURCES (comma-separated)
# Salt pillar: synapse:context_sources
#
# Available sources: gtd, calendar, context, time, sre, fleet, history, internal_docs
config :bot_army_synapse,
  context_sources: [
    "gtd",
    "calendar",
    "context",
    "time",
    "sre",
    "fleet",
    "history",
    "internal_docs"
  ],
  context_modules: %{
    "gtd" => BotArmySynapse.Context.GTD,
    "calendar" => BotArmySynapse.Context.Calendar,
    "fleet" => BotArmySynapse.Context.Fleet,
    "fleet_health" => BotArmySynapse.Context.FleetHealth,
    "goals" => BotArmySynapse.Context.Goals,
    "sre" => BotArmySynapse.Context.SRE,
    "internal_docs" => BotArmySynapse.Context.InternalDocs
  }

# NATS request timeouts (milliseconds)
config :bot_army_synapse,
  context_timeout: 3000,
  llm_timeout: 30000,
  internal_docs_timeout: 4500,
  internal_docs_result_limit: 5,
  include_internal_docs_in_chat: true

# Optional: after internal_docs.query, fetch full chunks via internal_docs.chunk.get (time-bounded)
config :bot_army_synapse,
  internal_docs_expand_chunks: false,
  internal_docs_expand_max_chunks: 2,
  internal_docs_expand_max_chars: 32_000,
  internal_docs_expand_neighbors_before: 0,
  internal_docs_expand_neighbors_after: 0,
  internal_docs_expand_budget_ms: 1_200,
  internal_docs_expand_per_chunk_timeout_ms: 600,
  internal_docs_expanded_prompt_chars: 12_000,
  internal_docs_snippet_prompt_chars: 800

# Run retention for persisted resumable runs
config :bot_army_synapse,
  run_retention_enabled: true,
  run_retention_interval_ms: 60 * 60 * 1000,
  run_retention_hours: 72

# Time zone database for DateTime.now!/2 with non-UTC zones
config :elixir, :time_zone_database, Tz.TimeZoneDatabase

# Logger metadata keys for domain-specific context
config :logger, :default_formatter,
  metadata: [
    :timestamp,
    :level,
    :module,
    :function,
    :tenant_id,
    :poll_id,
    :bot_name,
    :action,
    :event,
    :error
  ]

# Import environment-specific config
if File.exists?("config/#{config_env()}.exs") do
  import_config "#{config_env()}.exs"
end
