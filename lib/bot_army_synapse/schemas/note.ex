defmodule BotArmySynapse.Schemas.Note do
  @moduledoc """
  Ecto schema for free-form notes in the knowledge graph.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "synapse_notes" do
    field(:content, :string)
    field(:tags, {:array, :string}, default: [])
    field(:tenant_id, :string)
    field(:user_id, :string)

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(note, attrs) do
    note
    |> cast(attrs, [:content, :tags, :tenant_id, :user_id])
    |> validate_required([:content, :tenant_id])
  end
end
