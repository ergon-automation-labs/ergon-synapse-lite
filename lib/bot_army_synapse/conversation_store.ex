defmodule BotArmySynapse.ConversationStore do
  @moduledoc """
  Session conversation memory for Synapse.

  Keeps the latest Q&A exchanges in process memory and persists them through
  `BotArmy.Memory` so a restarted Synapse can resume the same thread.
  """

  use GenServer

  @max_history_per_session 10

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def record_exchange(session_id, question, answer, opts \\ []) do
    tenant_id = Keyword.get(opts, :tenant_id, BotArmyRuntime.Tenant.default_tenant_id())
    GenServer.cast(__MODULE__, {:record, session_id, question, answer, tenant_id, opts})
  end

  def get_history(session_id, opts \\ []) do
    tenant_id = Keyword.get(opts, :tenant_id, BotArmyRuntime.Tenant.default_tenant_id())
    GenServer.call(__MODULE__, {:get_history, session_id, tenant_id, opts})
  end

  def clear_history(session_id, opts \\ []) do
    tenant_id = Keyword.get(opts, :tenant_id, BotArmyRuntime.Tenant.default_tenant_id())
    GenServer.cast(__MODULE__, {:clear, session_id, tenant_id, opts})
  end

  @impl true
  def init(_opts) do
    {:ok, %{sessions: %{}}}
  end

  @impl true
  def handle_cast({:record, session_id, question, answer, tenant_id, opts}, state) do
    exchange = %{
      question: question,
      answer: answer,
      at: DateTime.utc_now()
    }

    session_history =
      Map.get(state.sessions, session_id, [])
      |> Kernel.++([exchange])
      |> Enum.take(-@max_history_per_session)

    sessions = Map.put(state.sessions, session_id, session_history)
    persist_exchange(session_id, question, answer, tenant_id, opts)
    {:noreply, %{state | sessions: sessions}}
  end

  @impl true
  def handle_cast({:clear, session_id, tenant_id, opts}, state) do
    sessions = Map.delete(state.sessions, session_id)
    clear_persisted(session_id, tenant_id, opts)
    {:noreply, %{state | sessions: sessions}}
  end

  @impl true
  def handle_call({:get_history, session_id, tenant_id, opts}, _from, state) do
    {history, sessions} =
      case Map.get(state.sessions, session_id) do
        exchanges when is_list(exchanges) and exchanges != [] ->
          {exchanges, state.sessions}

        _ ->
          loaded = load_persisted(session_id, tenant_id, opts)
          {loaded, Map.put(state.sessions, session_id, loaded)}
      end

    {:reply, history, %{state | sessions: sessions}}
  end

  defp persist_exchange(session_id, question, answer, tenant_id, opts) do
    memory_opts =
      opts
      |> Keyword.put(:tenant_id, tenant_id)
      |> Keyword.put(:limit, @max_history_per_session)
      |> Keyword.put_new(:source, "synapse")

    Task.start(fn ->
      _ = BotArmy.Memory.record_exchange(session_id, question, answer, memory_opts)
      :ok
    end)
  end

  defp load_persisted(session_id, tenant_id, opts) do
    memory_opts =
      opts
      |> Keyword.put(:tenant_id, tenant_id)
      |> Keyword.put(:limit, @max_history_per_session)

    BotArmy.Memory.list(session_id, memory_opts)
  end

  defp clear_persisted(session_id, tenant_id, opts) do
    memory_opts =
      opts
      |> Keyword.put(:tenant_id, tenant_id)
      |> Keyword.put(:kind, "exchange")

    Task.start(fn ->
      _ = BotArmy.Memory.clear(session_id, memory_opts)
      :ok
    end)
  end
end
