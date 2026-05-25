# ergon-synapse-lite

Lightweight RAG and context-gathering engine for Bot Army — decision support without autonomous scheduling.

This repo contains the **core Synapse engine** with general-purpose context sources. Specialized features (pi-go autonomy, TTRPG chronicles, factory coordination, GTD polling, tavern narration) live in the private downstream package.

## What's Included

### Core Engine

- **Orchestrator** — Gathers context from pluggable sources, formats prompt, calls LLM, routes response
- **DecisionEngine** — Rules-based evaluation of fleet events, triggers proactive sessions
- **ContextHydrationServer** — Keeps tenant-scoped context warm from heartbeat/signal events
- **ContextRegistry** — Pluggable context source discovery (add sources via config)
- **KnowledgeStore** — Event, link, and note CRUD for the knowledge graph (PostgreSQL)
- **ConversationStore** — Per-session Q&A memory
- **RunStore** — Resumable request state for multi-turn interactions
- **EventHistory** — Rolling window of 200 fleet events with 24h TTL
- **MemoryBroker** — Per-user timeline memory in ETS
- **ReflectionEngine** — Adaptive source pruning based on context ROI
- **IntentEvaluator** — Accumulated context threshold evaluation for proactive messaging

### General Context Sources (7)

| Source | Module | NATS Dependency |
|--------|--------|----------------|
| `gtd` | `Context.GTD` | `gtd.task.list` |
| `calendar` | `Context.Calendar` | `calendar.event.list` |
| `fleet` | `Context.Fleet` | PulseListener ETS |
| `fleet_health` | `Context.FleetHealth` | PulseListener ETS |
| `goals` | `Context.Goals` | `gtd.project.list` |
| `sre` | `Context.SRE` | `sre.alerts.active` |
| `internal_docs` | `Context.InternalDocs` | `internal_docs.query` |

Built-in sources (no module needed): `context` (context broker state), `time` (time-of-day), `history` (event history).

### Gossip / Social

- **GossipCoordinator** — Cross-bot intent/gossip resolution
- **GossipScheduler** — Periodic LLM check for proactive messaging

### Fleet Health

- **PulseListener** — Aggregates health pulses from all bots into real-time fleet state
- **PulsePublisher** — Periodic liveness pulse

## What's NOT Included (stays private)

- Pi-go autonomy scheduler and task dispatcher
- Factory decision coordinator and outcome tracking
- TTRPG campaign handler, GM actions, tavern narrator
- GTD polling (army opinion polls)
- PARA capture (inbox file writing)
- Suggestion reporter (periodic GTD digests to Discord)
- Daily command overrides
- Narrator budget and chronicle flavor

## Context Registry (Extensibility)

Add custom context sources via config:

```elixir
config :bot_army_synapse, :context_modules, %{
  "gtd" => BotArmySynapse.Context.GTD,
  "calendar" => BotArmySynapse.Context.Calendar,
  "fleet" => BotArmySynapse.Context.Fleet,
  "my_source" => MyApp.Context.MySource  # your extension
}
```

Each context module must implement `get_context/0` (returning a map or nil).

## NATS Subjects

Core request/reply subjects (lite):

| Subject | Description |
|---------|-------------|
| `synapse.query` | Main Q&A entry point |
| `synapse.analyze` | Context analysis without LLM |
| `synapse.continue` | Continue a multi-turn run |
| `synapse.feedback` | Record user feedback on a run |
| `synapse.log.create` | Create a knowledge graph event |
| `synapse.run.get` / `.list` | Query run state |
| `memory.user.recall` / `.summary` | User timeline memory |
| `llm.context.analyze` | LLM-powered context analysis |

## Setup

```bash
mix deps.get
mix ecto.create
mix ecto.migrate
```

## Running Tests

```bash
mix test
```

## License

Apache 2.0