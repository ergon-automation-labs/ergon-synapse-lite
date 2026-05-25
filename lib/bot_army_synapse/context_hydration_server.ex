defmodule BotArmySynapse.ContextHydrationServer do
  @moduledoc """
  Keeps Synapse context "warm" by ingesting heartbeat/signal events from NATS.

  This server maintains a per-tenant in-memory snapshot:
  - services health/capabilities
  - active risks
  - task verification signals
  - current context snapshot
  """

  use GenServer
  @behaviour BotArmySynapse.ContextHydration
  require Logger

  @reconnect_delay_ms 5_000
  @dedupe_window_seconds 600

  @subjects [
    "system.health",
    "system.capability.snapshot",
    "system.risk.signal",
    "context.state.current",
    "task.signal.verification",
    "ops.deploy.complete",
    "ops.deploy.failed",
    "ops.deploy.started"
  ]

  @ttl_seconds %{
    "system.health" => 90,
    "system.capability.snapshot" => 900,
    "system.risk.signal" => 300,
    "task.signal.verification" => 3600,
    "context.state.current" => 600,
    "ops.deploy.complete" => 1800,
    "ops.deploy.failed" => 1800,
    "ops.deploy.started" => 1800
  }

  # API
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl BotArmySynapse.ContextHydration
  def ingest_event(event) when is_map(event) do
    GenServer.call(__MODULE__, {:ingest_event, event})
  catch
    :exit, reason -> {:error, reason}
  end

  @impl BotArmySynapse.ContextHydration
  def current_snapshot(tenant_id) when is_binary(tenant_id) do
    GenServer.call(__MODULE__, {:current_snapshot, tenant_id})
  catch
    :exit, _ -> {:error, :not_found}
  end

  @impl BotArmySynapse.ContextHydration
  def risk_posture(tenant_id) when is_binary(tenant_id) do
    GenServer.call(__MODULE__, {:risk_posture, tenant_id})
  catch
    :exit, _ -> {:error, :not_found}
  end

  @impl BotArmySynapse.ContextHydration
  def stale_sources(tenant_id) when is_binary(tenant_id) do
    GenServer.call(__MODULE__, {:stale_sources, tenant_id})
  catch
    :exit, _ -> {:error, :not_found}
  end

  # GenServer
  @impl true
  def init(_opts) do
    state = %{
      subscriptions: [],
      tenants: %{},
      dedupe: %{}
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    case GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000) do
      {:ok, conn} ->
        BotArmyRuntime.NATS.Connection.subscribe_to_status()
        subscribe_topics(conn, state)

      {:error, _reason} ->
        Process.send_after(self(), :reconnect, @reconnect_delay_ms)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:msg, msg}, state) do
    with {:ok, decoded} <- Jason.decode(msg.body),
         :ok <- validate_envelope(decoded) do
      {_result, new_state} = ingest(decoded, state)
      {:noreply, new_state}
    else
      _ -> {:noreply, state}
    end
  end

  @impl true
  def handle_info({:nats, :disconnected}, state) do
    Process.send_after(self(), :reconnect, @reconnect_delay_ms)
    {:noreply, %{state | subscriptions: []}}
  end

  @impl true
  def handle_info({:nats, :connected}, state) do
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_info(:reconnect, state) do
    {:noreply, state, {:continue, :connect}}
  end

  @impl true
  def handle_call({:ingest_event, event}, _from, state) do
    {result, new_state} = ingest(event, state)
    {:reply, result, new_state}
  end

  def handle_call({:current_snapshot, tenant_id}, _from, state) do
    case Map.get(state.tenants, tenant_id) do
      nil -> {:reply, {:error, :not_found}, state}
      snapshot -> {:reply, {:ok, snapshot}, state}
    end
  end

  def handle_call({:risk_posture, tenant_id}, _from, state) do
    case Map.get(state.tenants, tenant_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      snapshot ->
        active_risks = snapshot.risks.active |> Map.values()

        posture = %{
          "total_active" => length(active_risks),
          "critical" => Enum.count(active_risks, &(&1["severity"] == "critical")),
          "high" => Enum.count(active_risks, &(&1["severity"] == "high")),
          "medium" => Enum.count(active_risks, &(&1["severity"] == "medium")),
          "low" => Enum.count(active_risks, &(&1["severity"] == "low")),
          "stale_sources" => stale_source_list(snapshot)
        }

        {:reply, {:ok, posture}, state}
    end
  end

  def handle_call({:stale_sources, tenant_id}, _from, state) do
    case Map.get(state.tenants, tenant_id) do
      nil -> {:reply, {:error, :not_found}, state}
      snapshot -> {:reply, {:ok, stale_source_list(snapshot)}, state}
    end
  end

  defp subscribe_topics(conn, state) do
    subs =
      Enum.reduce_while(@subjects, [], fn subject, acc ->
        case Gnat.sub(conn, self(), subject) do
          {:ok, sub} -> {:cont, [sub | acc]}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case subs do
      {:error, reason} ->
        Logger.warning("[ContextHydration] subscribe failed: #{inspect(reason)}")
        Process.send_after(self(), :reconnect, @reconnect_delay_ms)
        {:noreply, state}

      list ->
        {:noreply, %{state | subscriptions: list}}
    end
  end

  defp ingest(event, state) do
    with :ok <- validate_envelope(event),
         false <- duplicate?(event, state) do
      now = DateTime.utc_now()
      tenant_id = Map.fetch!(event, "tenant_id")
      subject = Map.fetch!(event, "event")
      source = Map.fetch!(event, "source")
      payload = Map.get(event, "payload", %{})

      tenant_snapshot =
        state.tenants
        |> Map.get(tenant_id, empty_snapshot())
        |> apply_subject_update(subject, source, payload, event)
        |> put_in([:meta, :last_updated_at], DateTime.to_iso8601(now))

      dedupe_key = dedupe_key(event)
      dedupe = prune_dedupe(Map.put(state.dedupe, dedupe_key, now), now)

      {:ok,
       %{state | tenants: Map.put(state.tenants, tenant_id, tenant_snapshot), dedupe: dedupe}}
    else
      true -> {:ok, state}
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp apply_subject_update(snapshot, "system.health", source, payload, event) do
    entry = payload |> Map.put("source", source) |> Map.put("received_at", event["timestamp"])
    put_in(snapshot, [:services, :health, source], entry)
  end

  defp apply_subject_update(snapshot, "system.capability.snapshot", source, payload, event) do
    entry = payload |> Map.put("source", source) |> Map.put("received_at", event["timestamp"])
    put_in(snapshot, [:services, :capabilities, source], entry)
  end

  defp apply_subject_update(snapshot, "system.risk.signal", _source, payload, event) do
    risk_id = Map.get(payload, "risk_id", event["event_id"])

    case Map.get(payload, "status") do
      "resolved" ->
        update_in(snapshot, [:risks, :active], &Map.delete(&1, risk_id))

      _ ->
        put_in(
          snapshot,
          [:risks, :active, risk_id],
          Map.put(payload, "received_at", event["timestamp"])
        )
    end
  end

  defp apply_subject_update(snapshot, "task.signal.verification", _source, payload, event) do
    task_id = Map.get(payload, "task_id", event["event_id"])

    put_in(
      snapshot,
      [:tasks, :verification, task_id],
      Map.put(payload, "received_at", event["timestamp"])
    )
  end

  defp apply_subject_update(snapshot, "context.state.current", source, payload, event) do
    value =
      payload
      |> Map.put("source", source)
      |> Map.put_new("updated_at", event["timestamp"])
      |> Map.put("received_at", event["timestamp"])

    put_in(snapshot, [:context, :current], value)
  end

  defp apply_subject_update(snapshot, "ops.deploy.complete", source, payload, event) do
    apply_deploy_update(snapshot, "ops.deploy.complete", source, payload, event)
  end

  defp apply_subject_update(snapshot, "ops.deploy.failed", source, payload, event) do
    apply_deploy_update(snapshot, "ops.deploy.failed", source, payload, event)
  end

  defp apply_subject_update(snapshot, "ops.deploy.started", source, payload, event) do
    apply_deploy_update(snapshot, "ops.deploy.started", source, payload, event)
  end

  defp apply_subject_update(snapshot, _other, _source, _payload, _event), do: snapshot

  defp empty_snapshot do
    %{
      services: %{health: %{}, capabilities: %{}},
      risks: %{active: %{}},
      tasks: %{verification: %{}},
      deployments: %{latest_by_bot: %{}},
      context: %{current: nil},
      meta: %{last_updated_at: nil}
    }
  end

  defp validate_envelope(event) when is_map(event) do
    required = ~w(event_id event schema_version timestamp source tenant_id payload)

    if Enum.all?(required, &Map.has_key?(event, &1)) do
      :ok
    else
      {:error, :invalid_envelope}
    end
  end

  defp validate_envelope(_), do: {:error, :invalid_envelope}

  defp duplicate?(event, state) do
    Map.has_key?(state.dedupe, dedupe_key(event))
  end

  defp dedupe_key(event) do
    payload = Map.get(event, "payload", %{})
    dedupe_key = Map.get(payload, "dedupe_key", event["event_id"])
    Enum.join([event["tenant_id"], event["source"], event["event"], dedupe_key], "::")
  end

  defp prune_dedupe(dedupe, now) do
    Enum.reduce(dedupe, %{}, fn {key, seen_at}, acc ->
      if DateTime.diff(now, seen_at, :second) <= @dedupe_window_seconds do
        Map.put(acc, key, seen_at)
      else
        acc
      end
    end)
  end

  defp stale_source_list(snapshot) do
    now = DateTime.utc_now()

    stale_health =
      snapshot.services.health
      |> Enum.flat_map(fn {source, value} ->
        stale_entry(now, "system.health", source, Map.get(value, "received_at"))
      end)

    stale_capabilities =
      snapshot.services.capabilities
      |> Enum.flat_map(fn {source, value} ->
        stale_entry(now, "system.capability.snapshot", source, Map.get(value, "received_at"))
      end)

    stale_risks =
      snapshot.risks.active
      |> Enum.flat_map(fn {_id, value} ->
        source = Map.get(value, "source", "unknown")
        stale_entry(now, "system.risk.signal", source, Map.get(value, "received_at"))
      end)

    stale_task_verification =
      snapshot.tasks.verification
      |> Enum.flat_map(fn {_task_id, value} ->
        source = Map.get(value, "source", "task.signal.verification")
        stale_entry(now, "task.signal.verification", source, Map.get(value, "received_at"))
      end)

    stale_context =
      case snapshot.context.current do
        nil ->
          []

        value ->
          source = Map.get(value, "source", "context")
          stale_entry(now, "context.state.current", source, Map.get(value, "updated_at"))
      end

    stale_deployments =
      snapshot.deployments.latest_by_bot
      |> Enum.flat_map(fn {_bot, value} ->
        signal_class = Map.get(value, "event", "ops.deploy.complete")
        source = Map.get(value, "source", "ops")
        stale_entry(now, signal_class, source, Map.get(value, "received_at"))
      end)

    stale_health ++
      stale_capabilities ++
      stale_risks ++ stale_task_verification ++ stale_context ++ stale_deployments
  end

  defp apply_deploy_update(snapshot, event_name, source, payload, event) do
    bot = Map.get(payload, "bot", "unknown")

    deploy_entry =
      payload
      |> Map.put("event", event_name)
      |> Map.put("source", source)
      |> Map.put("received_at", event["timestamp"])

    put_in(snapshot, [:deployments, :latest_by_bot, bot], deploy_entry)
  end

  defp stale_entry(now, signal_class, source, timestamp) do
    ttl = Map.get(@ttl_seconds, signal_class, 300)

    case parse_dt(timestamp) do
      {:ok, dt} ->
        age = DateTime.diff(now, dt, :second)

        if age > ttl do
          [%{"signal_class" => signal_class, "source" => source, "age_seconds" => age}]
        else
          []
        end

      :error ->
        [%{"signal_class" => signal_class, "source" => source, "age_seconds" => :unknown}]
    end
  end

  defp parse_dt(nil), do: :error

  defp parse_dt(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> {:ok, dt}
      _ -> :error
    end
  end

  defp parse_dt(_), do: :error
end
