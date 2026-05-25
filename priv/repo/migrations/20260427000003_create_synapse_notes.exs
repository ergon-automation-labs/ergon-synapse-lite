defmodule BotArmySynapse.Repo.Migrations.CreateSynapseNotes do
  use Ecto.Migration

  def change do
    create table(:synapse_notes, primary_key: false) do
      add(:id, :binary_id, primary_key: true)
      add(:content, :text, null: false)
      add(:tags, {:array, :string}, default: [])
      add(:tenant_id, :string, null: false)
      add(:user_id, :string)

      timestamps(type: :utc_datetime_usec)
    end

    create(index(:synapse_notes, [:tenant_id]))
    create(index(:synapse_notes, [:tags], using: :gin))
  end
end
