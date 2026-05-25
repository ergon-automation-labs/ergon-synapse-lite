import Config

# No database or external services in tests
config :elixir, :time_zone_database, Tz.TimeZoneDatabase

config :sentry,
  enabled: false,
  json_library: JSON
