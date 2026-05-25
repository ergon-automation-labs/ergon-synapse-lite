defmodule BotArmySynapse.Repo.Migrations.CreateSynapseEvents do
  use Ecto.Migration

  def change do
    create table(:synapse_events, primary_key: false) do
      add(:id, :binary_id, primary_key: true)
      add(:event_type, :string, null: false)
      add(:summary, :text)
      add(:tenant_id, :string, null: false)
      add(:user_id, :string)
      add(:metadata, :map, default: %{})
      add(:occurred_at, :utc_datetime_usec)

      timestamps(type: :utc_datetime_usec)
    end

    create(index(:synapse_events, [:tenant_id]))
    create(index(:synapse_events, [:event_type]))
    create(index(:synapse_events, [:occurred_at]))
    create(index(:synapse_events, [:tenant_id, :event_type]))
  end
end
