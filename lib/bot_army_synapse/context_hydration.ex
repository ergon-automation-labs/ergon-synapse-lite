defmodule BotArmySynapse.ContextHydration do
  @moduledoc """
  Public contract for Synapse context hydration.

  Implementations ingest cross-bot NATS events and maintain a fast, tenant-scoped
  snapshot used by Synapse for low-latency risk and status responses.
  """

  @callback ingest_event(map()) :: :ok | {:error, term()}
  @callback current_snapshot(String.t()) :: {:ok, map()} | {:error, :not_found}
  @callback risk_posture(String.t()) :: {:ok, map()} | {:error, :not_found}
  @callback stale_sources(String.t()) :: {:ok, [map()]} | {:error, :not_found}
end
