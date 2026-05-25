defmodule BotArmySynapse.Schemas.Run do
  @moduledoc """
  Ecto schema for persisted Synapse run state.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "synapse_runs" do
    field(:run_id, :string)
    field(:route, :string)
    field(:status, :string)
    field(:tenant_id, :string)
    field(:user_id, :string)
    field(:session_id, :string)
    field(:question, :string)
    field(:reply_to, :string)
    field(:payload, :map, default: %{})
    field(:metadata, :map, default: %{})
    field(:last_error, :string)
    field(:last_response, :string)
    field(:last_response_at, :utc_datetime_usec)
    field(:updated_at_iso, :string)

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(run, attrs) do
    run
    |> cast(attrs, [
      :run_id,
      :route,
      :status,
      :tenant_id,
      :user_id,
      :session_id,
      :question,
      :reply_to,
      :payload,
      :metadata,
      :last_error,
      :last_response,
      :last_response_at,
      :updated_at_iso
    ])
    |> validate_required([:run_id, :status])
    |> unique_constraint(:run_id)
  end
end
