defmodule BotArmySynapse.Stores.KnowledgeStore do
  @moduledoc """
  CRUD and query interface for the Synapse knowledge graph.
  """

  alias BotArmySynapse.Repo
  alias BotArmySynapse.Schemas.{Event, Link, Note}
  import Ecto.Query

  # -- Events --

  def create_event(attrs) do
    %Event{}
    |> Event.changeset(attrs)
    |> Repo.insert()
  end

  def list_events(tenant_id, opts \\ []) do
    limit = Keyword.get(opts, :limit, 100)
    offset = Keyword.get(opts, :offset, 0)
    event_type = Keyword.get(opts, :event_type)
    from_time = Keyword.get(opts, :from)
    to_time = Keyword.get(opts, :to)

    Event
    |> where([e], e.tenant_id == ^tenant_id)
    |> maybe_filter_type(event_type)
    |> maybe_filter_time(from_time, to_time)
    |> order_by([e], desc: e.occurred_at)
    |> limit(^limit)
    |> offset(^offset)
    |> Repo.all()
  end

  defp maybe_filter_type(query, nil), do: query
  defp maybe_filter_type(query, type), do: where(query, [e], e.event_type == ^type)

  defp maybe_filter_time(query, nil, nil), do: query
  defp maybe_filter_time(query, from, nil), do: where(query, [e], e.occurred_at >= ^from)
  defp maybe_filter_time(query, nil, to), do: where(query, [e], e.occurred_at <= ^to)

  defp maybe_filter_time(query, from, to),
    do: where(query, [e], e.occurred_at >= ^from and e.occurred_at <= ^to)

  # -- Links --

  def create_link(attrs) do
    %Link{}
    |> Link.changeset(attrs)
    |> Repo.insert()
  end

  def get_links(tenant_id, entity_id, direction \\ :both) do
    case direction do
      :from ->
        Link
        |> where([l], l.tenant_id == ^tenant_id and l.from_id == ^entity_id)
        |> Repo.all()

      :to ->
        Link
        |> where([l], l.tenant_id == ^tenant_id and l.to_id == ^entity_id)
        |> Repo.all()

      :both ->
        Link
        |> where(
          [l],
          l.tenant_id == ^tenant_id and (l.from_id == ^entity_id or l.to_id == ^entity_id)
        )
        |> Repo.all()
    end
  end

  # -- Notes --

  def create_note(attrs) do
    %Note{}
    |> Note.changeset(attrs)
    |> Repo.insert()
  end

  def list_notes(tenant_id, opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)
    offset = Keyword.get(opts, :offset, 0)
    tag = Keyword.get(opts, :tag)

    Note
    |> where([n], n.tenant_id == ^tenant_id)
    |> maybe_filter_tag(tag)
    |> order_by([n], desc: n.inserted_at)
    |> limit(^limit)
    |> offset(^offset)
    |> Repo.all()
  end

  defp maybe_filter_tag(query, nil), do: query
  defp maybe_filter_tag(query, tag), do: where(query, [n], ^tag in n.tags)

  # -- Cross-domain query --

  def query(tenant_id, query_text, opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)
    from_time = Keyword.get(opts, :from)
    to_time = Keyword.get(opts, :to)
    event_type = Keyword.get(opts, :event_type)

    events =
      Event
      |> where([e], e.tenant_id == ^tenant_id)
      |> maybe_filter_type(event_type)
      |> maybe_filter_time(from_time, to_time)
      |> where([e], ilike(e.summary, ^"%#{query_text}%"))
      |> order_by([e], desc: e.occurred_at)
      |> limit(^limit)
      |> Repo.all()

    notes =
      Note
      |> where([n], n.tenant_id == ^tenant_id)
      |> where([n], ilike(n.content, ^"%#{query_text}%"))
      |> order_by([n], desc: n.inserted_at)
      |> limit(^limit)
      |> Repo.all()

    %{events: events, notes: notes}
  end
end
