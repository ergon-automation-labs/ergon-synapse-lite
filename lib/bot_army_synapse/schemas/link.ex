defmodule BotArmySynapse.Schemas.Link do
  @moduledoc """
  Ecto schema for cross-domain entity links in the knowledge graph.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "synapse_links" do
    field(:from_id, :string)
    field(:to_id, :string)
    field(:relation_type, :string)
    field(:confidence, :float, default: 1.0)
    field(:tenant_id, :string)

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(link, attrs) do
    link
    |> cast(attrs, [:from_id, :to_id, :relation_type, :confidence, :tenant_id])
    |> validate_required([:from_id, :to_id, :relation_type, :tenant_id])
    |> validate_number(:confidence, greater_than_or_equal_to: 0.0, less_than_or_equal_to: 1.0)
  end
end
