defmodule BotArmySynapse.Repo.Migrations.CreateSynapseLinks do
  use Ecto.Migration

  def change do
    create table(:synapse_links, primary_key: false) do
      add(:id, :binary_id, primary_key: true)
      add(:from_id, :string, null: false)
      add(:to_id, :string, null: false)
      add(:relation_type, :string, null: false)
      add(:confidence, :float, default: 1.0)
      add(:tenant_id, :string, null: false)

      timestamps(type: :utc_datetime_usec)
    end

    create(index(:synapse_links, [:tenant_id]))
    create(index(:synapse_links, [:from_id]))
    create(index(:synapse_links, [:to_id]))
    create(index(:synapse_links, [:tenant_id, :relation_type]))
    create(unique_index(:synapse_links, [:from_id, :to_id, :relation_type, :tenant_id]))
  end
end
