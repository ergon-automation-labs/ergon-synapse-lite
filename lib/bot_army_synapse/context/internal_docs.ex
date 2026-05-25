defmodule BotArmySynapse.Context.InternalDocs do
  @moduledoc """
  RAG-style context from `bot_army_internal_docs` via `internal_docs.query`.

  Only runs when the user includes `internal_docs` in `context_sources` and
  the current user message / question is non-empty. On failure, returns `nil`
  so the rest of context gathering continues.

  ## Optional chunk expansion

  When `:internal_docs_expand_chunks` is `true`, after a successful
  `internal_docs.query`, Synapse issues bounded `internal_docs.chunk.get`
  calls for the top `internal_docs_expand_max_chunks` hits so prompts can
  include more than query snippets. Expansion is time-budgeted
  (`internal_docs_expand_budget_ms` after the query returns) and each fetch
  uses `internal_docs_expand_per_chunk_timeout_ms`. Neighbor slices are
  optional via `internal_docs_expand_neighbors_before` /
  `internal_docs_expand_neighbors_after` (see internal_docs bot contract).
  """

  require Logger

  @default_limit 5
  @docs_search_subject "internal_docs.search"
  @docs_query_subject "internal_docs.query"

  @doc """
  Run a semantic search against the internal documentation index.

  `question` is typically the same string passed to `llm.context.analyze` or
  the user's chat line. Returns a map with `query`, `results`, and optional
  `fallback` or `nil` when disabled or on error.
  """
  def get_context(question) when question in [nil, ""], do: nil

  def get_context(question) when is_binary(question) do
    case String.trim(question) do
      "" -> nil
      q -> do_query(q)
    end
  end

  @doc """
  Format one internal_docs hit for LLM prompts (orchestrator / Discord).

  Uses `expanded_body` when present (from `internal_docs.chunk.get`); otherwise
  uses the `content` snippet from `internal_docs.query`. Character limits are
  controlled by `internal_docs_expanded_prompt_chars` and
  `internal_docs_snippet_prompt_chars`.
  """
  def format_hit_for_prompt(r) when is_map(r) do
    id = Map.get(r, "id", "")
    heading = Map.get(r, "heading", "")
    summary = Map.get(r, "summary", "")

    {body, max_chars} =
      case Map.get(r, "expanded_body") do
        text when is_binary(text) and text != "" ->
          m = Application.get_env(:bot_army_synapse, :internal_docs_expanded_prompt_chars, 12_000)
          {text, m}

        _ ->
          c = Map.get(r, "content", "")
          m = Application.get_env(:bot_army_synapse, :internal_docs_snippet_prompt_chars, 800)
          {c, m}
      end

    sum =
      if is_binary(summary) and summary != "",
        do: "\n    summary: #{String.slice(summary, 0, 240)}",
        else: ""

    "  • [id=#{id}] #{heading}#{sum}\n    #{String.slice(body, 0, max_chars)}"
  end

  defp do_query(question) do
    timeout = Application.get_env(:bot_army_synapse, :internal_docs_timeout, 4_500)
    t_query_start = System.monotonic_time(:millisecond)

    with {:ok, conn} <- GenServer.call(BotArmyRuntime.NATS.Connection, :get_connection, 5_000),
         {:ok, data} <- request_with_fallbacks(conn, question, timeout) do
      t_after_query = System.monotonic_time(:millisecond)

      if expand_chunks?() do
        budget =
          Application.get_env(:bot_army_synapse, :internal_docs_expand_budget_ms, 1_200)

        spent_on_query = t_after_query - t_query_start
        remaining_budget = max(0, budget - spent_on_query)
        deadline = t_after_query + remaining_budget

        max_n = Application.get_env(:bot_army_synapse, :internal_docs_expand_max_chunks, 2)

        max_chars =
          Application.get_env(:bot_army_synapse, :internal_docs_expand_max_chars, 32_000)

        before_n =
          Application.get_env(:bot_army_synapse, :internal_docs_expand_neighbors_before, 0)

        after_n =
          Application.get_env(:bot_army_synapse, :internal_docs_expand_neighbors_after, 0)

        per_ms =
          Application.get_env(
            :bot_army_synapse,
            :internal_docs_expand_per_chunk_timeout_ms,
            600
          )

        expanded_results =
          expand_top_results(
            conn,
            Map.get(data, "results", []),
            max_n,
            max_chars,
            before_n,
            after_n,
            deadline,
            per_ms
          )

        Map.put(data, "results", expanded_results)
      else
        data
      end
    else
      {:error, reason} ->
        Logger.debug("[InternalDocs] query failed: #{inspect(reason)}")
        nil
    end
  end

  defp request_with_fallbacks(conn, question, timeout) do
    limit =
      Application.get_env(:bot_army_synapse, :internal_docs_result_limit, @default_limit)

    variants = query_variants(question)

    case request_subject_with_variants(
           conn,
           @docs_search_subject,
           variants,
           limit,
           timeout,
           "keyword"
         ) do
      {:ok, data} ->
        {:ok, data}

      {:error, keyword_reason} ->
        Logger.debug("[InternalDocs] keyword search failed: #{inspect(keyword_reason)}")

        case request_subject_with_variants(
               conn,
               @docs_query_subject,
               variants,
               limit,
               timeout,
               "semantic"
             ) do
          {:ok, data} -> {:ok, data}
          {:error, semantic_reason} -> {:error, {:all_failed, keyword_reason, semantic_reason}}
        end
    end
  end

  defp request_subject_with_variants(conn, subject, variants, limit, timeout, fallback_label) do
    Enum.reduce_while(variants, {:error, :no_results}, fn query, _acc ->
      payload = %{"query" => query, "limit" => limit}

      case request_docs(conn, subject, payload, timeout) do
        {:ok, data} ->
          case data do
            %{"results" => results} when is_list(results) and results != [] ->
              shaped =
                data
                |> Map.put("query", query)
                |> Map.put("fallback", fallback_label)

              {:halt, {:ok, shaped}}

            _ ->
              {:cont, {:error, :no_results}}
          end

        {:error, reason} ->
          {:cont, {:error, reason}}
      end
    end)
  end

  defp request_docs(conn, subject, payload, timeout) do
    body = Jason.encode!(payload)

    case Gnat.request(conn, subject, body, receive_timeout: timeout) do
      {:ok, response} ->
        case parse_response(extract_body(response), Map.get(payload, "query", "")) do
          data when is_map(data) -> {:ok, data}
          _ -> {:error, :no_results}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp expand_chunks? do
    Application.get_env(:bot_army_synapse, :internal_docs_expand_chunks, false) == true
  end

  defp expand_top_results(conn, results, max_n, max_chars, before_n, after_n, deadline_ms, per_ms) do
    results
    |> Enum.with_index()
    |> Enum.map(fn {hit, i} ->
      if i >= max_n or System.monotonic_time(:millisecond) >= deadline_ms do
        hit
      else
        id = Map.get(hit, "id")

        if not is_binary(id) or id == "" do
          hit
        else
          now = System.monotonic_time(:millisecond)
          time_left = deadline_ms - now

          if time_left < 30 do
            hit
          else
            receive_ms = min(per_ms, max(30, time_left))

            case fetch_chunk(conn, id, max_chars, before_n, after_n, receive_ms) do
              {:ok, text} -> Map.put(hit, "expanded_body", text)
              _ -> hit
            end
          end
        end
      end
    end)
  end

  defp fetch_chunk(conn, chunk_id, max_chars, before_n, after_n, receive_ms) do
    req_body =
      Jason.encode!(
        BotArmySynapse.build_envelope("internal_docs.chunk.get", %{
          "chunk_id" => chunk_id,
          "max_chars" => max_chars,
          "before" => before_n,
          "after" => after_n
        })
      )

    case Gnat.request(conn, "internal_docs.chunk.get", req_body, receive_timeout: receive_ms) do
      {:ok, response} ->
        case parse_chunk_get_response(extract_body(response)) do
          {:ok, inner} ->
            {:ok, build_expanded_text(Map.get(inner, "chunk"), Map.get(inner, "neighbors", []))}

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp parse_chunk_get_response(body) when is_binary(body) do
    with {:ok, decoded} <- Jason.decode(body) do
      inner = BotArmySynapse.extract_payload(decoded)

      cond do
        Map.get(inner, "ok") in [true, "true"] -> {:ok, inner}
        Map.get(decoded, "ok") in [true, "true"] -> {:ok, BotArmySynapse.extract_payload(decoded)}
        true -> :error
      end
    else
      _ -> :error
    end
  end

  defp parse_chunk_get_response(_), do: :error

  defp build_expanded_text(nil, _), do: ""

  defp build_expanded_text(chunk, neighbors) when is_map(chunk) do
    main = Map.get(chunk, "content", "")

    extra =
      (neighbors || [])
      |> Enum.map(&Map.get(&1, "content", ""))
      |> Enum.reject(&(&1 == ""))

    case extra do
      [] -> main
      _ -> Enum.join([main | extra], "\n\n---\n\n")
    end
  end

  defp query_variants(question) do
    trimmed = String.trim(question || "")

    if trimmed == "" do
      []
    else
      normalized =
        trimmed
        |> String.downcase()
        |> String.replace(~r/[^a-z0-9]+/u, " ")
        |> String.split()
        |> Enum.join(" ")

      variants =
        [trimmed, normalized]
        |> Enum.reject(&(&1 in [nil, ""]))
        |> Enum.uniq()

      if String.contains?(normalized, "bot army") do
        Enum.uniq(variants ++ ["bot army"])
      else
        variants
      end
    end
  end

  defp extract_body(%{body: body}) when is_binary(body), do: body
  defp extract_body(body) when is_binary(body), do: body

  defp parse_response(body, question) when is_binary(body) do
    with {:ok, decoded} <- Jason.decode(body),
         response <- BotArmySynapse.extract_payload(decoded),
         true <- Map.get(response, "ok", Map.get(decoded, "ok")) in [true, "true"] do
      results = Map.get(response, "results", Map.get(decoded, "results", []))

      if is_list(results) and results != [] do
        %{
          "query" => question,
          "results" => results,
          "fallback" => Map.get(response, "fallback", Map.get(decoded, "fallback"))
        }
      else
        nil
      end
    else
      _ -> nil
    end
  end

  defp parse_response(_, _), do: nil
end
