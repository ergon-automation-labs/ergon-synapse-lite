defmodule BotArmySynapse.RunStore do
  @moduledoc """
  In-memory run state tracking for resumable Synapse requests.
  """

  use GenServer

  @max_runs 500

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def put_run(run_id, attrs) when is_binary(run_id) and is_map(attrs) do
    GenServer.cast(__MODULE__, {:put_run, run_id, attrs})
  end

  def get_run(run_id) when is_binary(run_id) do
    GenServer.call(__MODULE__, {:get_run, run_id})
  end

  def list_runs(limit \\ 50, tenant_id \\ nil) when is_integer(limit) and limit > 0 do
    GenServer.call(__MODULE__, {:list_runs, limit, tenant_id})
  end

  def mark_status(run_id, status, extra \\ %{}) when is_binary(run_id) and is_map(extra) do
    GenServer.cast(__MODULE__, {:mark_status, run_id, status, extra})
  end

  @impl true
  def init(_opts) do
    {:ok, %{runs: %{}, order: []}}
  end

  @impl true
  def handle_cast({:put_run, run_id, attrs}, state) do
    run = Map.merge(%{"run_id" => run_id}, attrs)
    runs = Map.put(state.runs, run_id, run)
    order = [run_id | Enum.reject(state.order, &(&1 == run_id))]
    {runs, order} = trim_runs(runs, order)
    persist_run(run)
    {:noreply, %{state | runs: runs, order: order}}
  end

  @impl true
  def handle_cast({:mark_status, run_id, status, extra}, state) do
    run =
      state.runs
      |> Map.get(run_id, %{"run_id" => run_id})
      |> Map.put("status", to_string(status))
      |> Map.put("updated_at", DateTime.utc_now() |> DateTime.to_iso8601())
      |> Map.merge(extra)

    runs = Map.put(state.runs, run_id, run)
    order = [run_id | Enum.reject(state.order, &(&1 == run_id))]
    {runs, order} = trim_runs(runs, order)
    persist_run(run)
    {:noreply, %{state | runs: runs, order: order}}
  end

  @impl true
  def handle_call({:get_run, run_id}, _from, state) do
    run =
      Map.get(state.runs, run_id) ||
        case BotArmySynapse.Stores.RunRecordStore.get_run(run_id) do
          nil -> nil
          persisted -> persisted_to_map(persisted)
        end

    {:reply, run, state}
  end

  @impl true
  def handle_call({:list_runs, limit, tenant_id}, _from, state) do
    in_memory_runs =
      state.order
      |> Enum.take(limit)
      |> Enum.map(&Map.get(state.runs, &1))
      |> maybe_filter_tenant(tenant_id)
      |> Enum.reject(&is_nil/1)

    runs =
      if in_memory_runs == [] do
        case tenant_id do
          tenant when is_binary(tenant) and tenant != "" ->
            BotArmySynapse.Stores.RunRecordStore.recent_runs_for_tenant(tenant, limit)
            |> Enum.map(&persisted_to_map/1)

          _ ->
            BotArmySynapse.Stores.RunRecordStore.recent_runs(limit)
            |> Enum.map(&persisted_to_map/1)
        end
      else
        in_memory_runs
      end

    {:reply, runs, state}
  end

  defp trim_runs(runs, order) do
    if length(order) <= @max_runs do
      {runs, order}
    else
      kept_order = Enum.take(order, @max_runs)
      kept_ids = MapSet.new(kept_order)
      trimmed_runs = Map.take(runs, MapSet.to_list(kept_ids))
      {trimmed_runs, kept_order}
    end
  end

  defp persist_run(run) do
    attrs = %{
      "run_id" => run["run_id"],
      "route" => run["route"],
      "status" => run["status"],
      "tenant_id" => run["tenant_id"],
      "user_id" => run["user_id"],
      "session_id" => run["session_id"],
      "question" => run["question"],
      "reply_to" => run["reply_to"],
      "payload" => Map.get(run, "payload", %{}),
      "metadata" => Map.get(run, "metadata", %{}),
      "last_error" => run["error"],
      "last_response" => run["last_response"],
      "last_response_at" => parse_iso_datetime(run["last_response_at"]),
      "updated_at_iso" => run["updated_at"]
    }

    Task.start(fn ->
      _ = BotArmySynapse.Stores.RunRecordStore.upsert_run(attrs)
      :ok
    end)
  end

  defp parse_iso_datetime(nil), do: nil
  defp parse_iso_datetime(""), do: nil

  defp parse_iso_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _} -> dt
      _ -> nil
    end
  end

  defp parse_iso_datetime(_), do: nil

  defp persisted_to_map(run) do
    %{
      "run_id" => run.run_id,
      "route" => run.route,
      "status" => run.status,
      "tenant_id" => run.tenant_id,
      "user_id" => run.user_id,
      "session_id" => run.session_id,
      "question" => run.question,
      "reply_to" => run.reply_to,
      "payload" => run.payload || %{},
      "metadata" => run.metadata || %{},
      "error" => run.last_error,
      "last_response" => run.last_response,
      "last_response_at" =>
        if(run.last_response_at, do: DateTime.to_iso8601(run.last_response_at)),
      "updated_at" => run.updated_at_iso
    }
  end

  defp maybe_filter_tenant(runs, tenant_id) when is_binary(tenant_id) and tenant_id != "" do
    Enum.filter(runs, fn run -> is_map(run) and run["tenant_id"] == tenant_id end)
  end

  defp maybe_filter_tenant(runs, _tenant_id), do: runs
end
