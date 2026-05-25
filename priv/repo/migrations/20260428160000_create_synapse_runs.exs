defmodule BotArmySynapse.Repo.Migrations.CreateSynapseRuns do
  use Ecto.Migration

  def change do
    create table(:synapse_runs, primary_key: false) do
      add(:id, :binary_id, primary_key: true)
      add(:run_id, :string, null: false)
      add(:route, :string)
      add(:status, :string, null: false)
      add(:tenant_id, :string)
      add(:user_id, :string)
      add(:session_id, :string)
      add(:question, :text)
      add(:reply_to, :string)
      add(:payload, :map, default: %{})
      add(:metadata, :map, default: %{})
      add(:last_error, :text)
      add(:last_response, :text)
      add(:last_response_at, :utc_datetime_usec)
      add(:updated_at_iso, :string)

      timestamps(type: :utc_datetime_usec)
    end

    create(unique_index(:synapse_runs, [:run_id]))
    create(index(:synapse_runs, [:status]))
    create(index(:synapse_runs, [:tenant_id]))
    create(index(:synapse_runs, [:updated_at]))
  end
end
