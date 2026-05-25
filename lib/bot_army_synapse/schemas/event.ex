defmodule BotArmySynapse.Schemas.Event do
  @moduledoc """
  Ecto schema for persisted fleet events in the knowledge graph.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "synapse_events" do
    field(:event_type, :string)
    field(:summary, :string)
    field(:tenant_id, :string)
    field(:user_id, :string)
    field(:metadata, :map, default: %{})
    field(:occurred_at, :utc_datetime_usec)

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(event, attrs) do
    event
    |> cast(attrs, [:event_type, :summary, :tenant_id, :user_id, :metadata, :occurred_at])
    |> validate_required([:event_type, :tenant_id])
  end
end
