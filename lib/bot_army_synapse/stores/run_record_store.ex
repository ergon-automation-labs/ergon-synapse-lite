defmodule BotArmySynapse.Stores.RunRecordStore do
  @moduledoc """
  Persistence layer for resumable Synapse run state.
  """

  import Ecto.Query

  alias BotArmySynapse.Repo
  alias BotArmySynapse.Schemas.Run

  def upsert_run(attrs) when is_map(attrs) do
    run_id = Map.get(attrs, "run_id")

    case Repo.get_by(Run, run_id: run_id) do
      nil ->
        %Run{}
        |> Run.changeset(attrs)
        |> Repo.insert()

      existing ->
        existing
        |> Run.changeset(attrs)
        |> Repo.update()
    end
  end

  def get_run(run_id) when is_binary(run_id) do
    Repo.get_by(Run, run_id: run_id)
  end

  def recent_runs(limit \\ 50) do
    Run
    |> order_by([r], desc: r.updated_at)
    |> limit(^limit)
    |> Repo.all()
  end

  def recent_runs_for_tenant(tenant_id, limit \\ 50)

  def recent_runs_for_tenant(tenant_id, limit)
      when is_binary(tenant_id) and tenant_id != "" do
    Run
    |> where([r], r.tenant_id == ^tenant_id)
    |> order_by([r], desc: r.updated_at)
    |> limit(^limit)
    |> Repo.all()
  end

  def recent_runs_for_tenant(_tenant_id, limit), do: recent_runs(limit)

  def delete_runs_older_than(hours) when is_integer(hours) and hours > 0 do
    cutoff = DateTime.utc_now() |> DateTime.add(-(hours * 3600), :second)

    Run
    |> where([r], r.updated_at < ^cutoff)
    |> Repo.delete_all()
  end
end
