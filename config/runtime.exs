import Config

# Database configuration at runtime
# Priority: BOT_ARMY_SYNAPSE_DB_* (set by Salt/Jenkins) > DATABASE_* (from .env for local dev) > defaults
config :bot_army_synapse, BotArmySynapse.Repo,
  database:
    System.get_env("BOT_ARMY_SYNAPSE_DB_NAME") || System.get_env("DATABASE_NAME") ||
      "ergon_synapse_dev",
  hostname:
    System.get_env("BOT_ARMY_SYNAPSE_DB_HOST") || System.get_env("DATABASE_HOST") || "localhost",
  port:
    String.to_integer(
      System.get_env("BOT_ARMY_SYNAPSE_DB_PORT") || System.get_env("DATABASE_PORT") || "30003"
    ),
  username:
    System.get_env("BOT_ARMY_SYNAPSE_DB_USER") || System.get_env("DATABASE_USER") || "postgres",
  password:
    System.get_env("BOT_ARMY_SYNAPSE_DB_PASSWORD") || System.get_env("DATABASE_PASSWORD") ||
      "postgres",
  pool_size:
    String.to_integer(
      System.get_env("BOT_ARMY_SYNAPSE_DB_POOL_SIZE") ||
        System.get_env("DATABASE_POOL_SIZE") ||
        "5"
    ),
  queue_target:
    String.to_integer(
      System.get_env("BOT_ARMY_SYNAPSE_DB_QUEUE_TARGET_MS") ||
        System.get_env("DATABASE_QUEUE_TARGET_MS") ||
        "5000"
    ),
  queue_interval:
    String.to_integer(
      System.get_env("BOT_ARMY_SYNAPSE_DB_QUEUE_INTERVAL_MS") ||
        System.get_env("DATABASE_QUEUE_INTERVAL_MS") ||
        "1000"
    ),
  ssl: false

# Learning library configuration (uses same database as this bot)
config :bot_army_library_learning, ecto_repos: [BotArmyLearning.Repo]

config :bot_army_library_learning, BotArmyLearning.Repo,
  database:
    System.get_env("BOT_ARMY_SYNAPSE_DB_NAME") || System.get_env("DATABASE_NAME") ||
      "ergon_synapse_dev",
  hostname:
    System.get_env("BOT_ARMY_SYNAPSE_DB_HOST") || System.get_env("DATABASE_HOST") || "localhost",
  port:
    String.to_integer(
      System.get_env("BOT_ARMY_SYNAPSE_DB_PORT") || System.get_env("DATABASE_PORT") || "30003"
    ),
  username:
    System.get_env("BOT_ARMY_SYNAPSE_DB_USER") || System.get_env("DATABASE_USER") || "postgres",
  password:
    System.get_env("BOT_ARMY_SYNAPSE_DB_PASSWORD") || System.get_env("DATABASE_PASSWORD") ||
      "postgres",
  pool_size: 5,
  ssl: false

# NATS configuration for bot_army_runtime
nats_host = System.get_env("NATS_HOST") || "localhost"
nats_port = String.to_integer(System.get_env("NATS_PORT") || "4223")

config :bot_army_library_runtime, :nats,
  servers: [{nats_host, nats_port}],
  ping_interval: 30_000,
  max_reconnect_attempts: 10,
  reconnect_delay_ms: 1000

# Allow persona override via environment variable
if persona = System.get_env("SYNAPSE_DISCORD_PERSONA") do
  config :bot_army_synapse, :discord_persona, persona
end

# Optional runtime overrides for Synapse -> pi-go autonomous scheduler
config :bot_army_synapse,
  pi_go_dispatch_enabled:
    String.downcase(System.get_env("PI_GO_DISPATCH_ENABLED", "false")) in ["1", "true", "yes"],
  pi_go_dispatch_interval_ms:
    String.to_integer(System.get_env("PI_GO_DISPATCH_INTERVAL_MS", "60000")),
  pi_go_dispatch_limit: String.to_integer(System.get_env("PI_GO_DISPATCH_LIMIT", "50")),
  pi_go_dispatch_max_pages: String.to_integer(System.get_env("PI_GO_DISPATCH_MAX_PAGES", "12")),
  pi_go_dispatch_max_dispatches:
    String.to_integer(System.get_env("PI_GO_DISPATCH_MAX_DISPATCHES", "10")),
  pi_go_dispatch_use_factory_fixer:
    String.downcase(System.get_env("PI_GO_DISPATCH_USE_FACTORY_FIXER", "false")) in [
      "1",
      "true",
      "yes"
    ],
  pi_go_factory_fixer_subject:
    System.get_env("PI_GO_FACTORY_FIXER_SUBJECT", "factory.fixer.request"),
  pi_go_autonomy_enabled:
    String.downcase(System.get_env("PI_GO_AUTONOMY_ENABLED", "false")) in ["1", "true", "yes"],
  pi_go_autonomy_interval_ms:
    String.to_integer(System.get_env("PI_GO_AUTONOMY_INTERVAL_MS", "60000")),
  pi_go_autonomy_cooldown_ms:
    String.to_integer(System.get_env("PI_GO_AUTONOMY_COOLDOWN_MS", "300000")),
  pi_go_autonomy_context_change_min_interval_ms:
    String.to_integer(System.get_env("PI_GO_AUTONOMY_CONTEXT_CHANGE_MIN_INTERVAL_MS", "120000")),
  pi_go_autonomy_pulse_max_age_seconds:
    String.to_integer(System.get_env("PI_GO_AUTONOMY_PULSE_MAX_AGE_SECONDS", "90")),
  pi_go_autonomy_max_in_flight:
    String.to_integer(System.get_env("PI_GO_AUTONOMY_MAX_IN_FLIGHT", "1")),
  pi_go_autonomy_require_llm_responder:
    String.downcase(System.get_env("PI_GO_AUTONOMY_REQUIRE_LLM_RESPONDER", "true")) in [
      "1",
      "true",
      "yes"
    ],
  pi_go_autonomy_work_request_subject:
    System.get_env("PI_GO_AUTONOMY_WORK_REQUEST_SUBJECT", "synapse.pi_go.request_work"),
  pi_go_autonomy_llm_subject:
    System.get_env("PI_GO_AUTONOMY_LLM_SUBJECT", "pi-go.llm.request.chat"),
  pi_go_autonomy_llm_check_timeout_ms:
    String.to_integer(System.get_env("PI_GO_AUTONOMY_LLM_CHECK_TIMEOUT_MS", "1500")),
  pi_go_autonomy_prompt:
    System.get_env(
      "PI_GO_AUTONOMY_PROMPT",
      "Scan recent system context and pick the most useful next action."
    ),
  pi_go_discord_channel_id: System.get_env("PI_GO_DISCORD_CHANNEL")

# Optional runtime overrides for Synapse suggestion reporter
config :bot_army_synapse,
  suggestion_reporter_enabled:
    System.get_env("SUGGESTION_REPORTER_ENABLED", "true") in ["1", "true", "yes"],
  suggestion_reporter_interval_ms:
    String.to_integer(System.get_env("SUGGESTION_REPORTER_INTERVAL_MS", "21600000")),
  suggestion_reporter_channel_id: System.get_env("SUGGESTION_REPORTER_CHANNEL_ID"),
  suggestion_reactions_enabled:
    System.get_env("SUGGESTION_REACTIONS_ENABLED", "false") in ["1", "true", "yes"],
  suggestion_stale_goal_days:
    String.to_integer(System.get_env("SUGGESTION_STALE_GOAL_DAYS", "7")),
  suggestion_stale_task_active_days:
    String.to_integer(System.get_env("SUGGESTION_STALE_TASK_ACTIVE_DAYS", "7")),
  suggestion_stale_task_inbox_hours:
    String.to_integer(System.get_env("SUGGESTION_STALE_TASK_INBOX_HOURS", "24"))
