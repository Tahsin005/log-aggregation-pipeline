# Log Aggregation Pipeline

A distributed log aggregation system built to explore event-driven architecture with RabbitMQ topic exchanges, Go producers/consumers, and a Django-powered read dashboard.

Multiple mock services publish structured logs → RabbitMQ routes them by service and severity using a topic exchange → Go consumers persist them to Postgres and raise alerts on high-severity events → a Django admin dashboard provides filtering and search with zero custom frontend code.

## Architecture

```

┌─────────────┐        ┌──────────────────────────┐
│ Go Producer │──────▶│  logs_topic_exchange       │
│ (mock svcs) │  publish  (topic, routing key:      │
└─────────────┘        │   <service>.<severity>)    │
                        └──────────┬────────────────┘
                                   │ bind "#"
                     ┌─────────────┴──────────────┐
                     ▼                             ▼
          ┌─────────────────────┐       ┌──────────────────────┐
          │ storage_writer       │       │ alerter                │
          │ (binds "#")           │       │ (binds *.critical,    │
          │ writes to Postgres    │       │  *.error)              │
          └──────────┬────────────┘       │ logs to stdout         │
                     │                    └──────────────────────┘
              insert fails
                     │
                     ▼
        ┌─────────────────────────┐
        │ Retry loop:               │
        │ retry exchange (fanout)   │
        │ → TTL queue (5s)          │
        │ → back to topic exchange  │
        └──────────┬────────────────┘
           exhausted 3 attempts
                     │
                     ▼
        ┌─────────────────────────┐
        │ Final DLQ                 │
        │ (manual review / replay)  │
        └─────────────────────────┘

┌────────────────────┐
│ Django Admin         │  reads same Postgres "logs" table
│ (filter/search UI)   │  managed = False — never migrates it
└────────────────────┘

````

## Folder structure

```

log-aggregation-pipeline/
├── docker-compose.yml       # Postgres + RabbitMQ
├── db-init/
│   └── 001_create_logs_table.sql
├── producer/                 # mock service log generator
│   ├── main.go
│   ├── publisher.go
│   └── go.mod
├── consumer/                 # storage_writer + alerter, run together
│   ├── main.go
│   ├── storage_writer.go
│   ├── alerter.go
│   └── go.mod
├── replay/                   # one-shot CLI to replay messages from the final DLQ
│   ├── main.go
│   └── go.mod
└── dashboard/                 # Django project (admin as the UI)
├── manage.py
├── logs_dashboard/
│   └── settings.py
└── logs/
├── models.py
└── admin.py

````

## Tech stack

- **Go** (`amqp091-go`, `pgx/v5`) — producer and consumer
- **RabbitMQ** — topic exchange for routing, fanout exchanges for the retry/DLQ loop
- **PostgreSQL** — single source of truth for stored logs
- **Django** — admin-only dashboard (`managed = False` model, no custom views/templates)
- **Docker Compose** — Postgres + RabbitMQ infra

## Design decisions

**Why a topic exchange?** Routing keys follow `<service>.<severity>` (e.g. `payment.critical`). This lets different consumers bind to different patterns off the *same* exchange without the producer knowing who's listening — `storage_writer` binds `#` (everything), `alerter` binds `*.critical` and `*.error` (only what it cares about). A direct or fanout exchange couldn't express this without either losing routing precision or forcing every consumer to filter client-side.

**Why Django admin instead of a custom dashboard?** The read side of this project is deliberately not the interesting part — filtering/searching structured rows is a solved problem. Django's admin gives sortable columns, sidebar filters, and search out of the box, which let the project's effort go into the RabbitMQ/Go side instead of rebuilding a table UI.

**Why `managed = False` on the Django model?** Two systems, one table, one owner. The Go consumer's `storage_writer` and the SQL in `db-init/` own the schema; Django is read/query-only and explicitly forbidden from creating, altering, or dropping that table via migrations. This avoids two migration systems fighting over the same table.

**Why manual ack (not auto-ack) in the consumers?** Auto-ack would mark a message as delivered the instant RabbitMQ hands it over — before we know if the Postgres insert actually succeeded. Manual ack means a message is only removed from the queue after it's durably stored (or deliberately given up on via the DLQ), which is what makes the retry/DLQ logic meaningful at all.

## Reliability: retry + dead letter queue

Failed inserts (e.g. Postgres briefly unavailable) don't get discarded or endlessly retried in a tight loop:

1. On failure, the message is `Nack`'d without requeue, which routes it to a **retry exchange**.
2. The retry exchange feeds a **queue with a 5-second TTL**. When the TTL expires, RabbitMQ dead-letters it back to the main topic exchange — landing back on the main queue.
3. This repeats up to **3 attempts**, tracked via RabbitMQ's built-in `x-death` header (no custom state needed).
4. After 3 failed attempts, the message is manually published to a **final DLQ** for inspection, with the original routing key preserved in a header (since the DLQ publish itself uses a fanout exchange).

### Replaying from the DLQ

```bash
cd replay
go mod tidy

# inspect without touching anything
go run . -dry-run

# republish everything back through the normal pipeline
go run .
````

## Known limitations (by design, for project scope)

* **Fixed 5s retry delay, not exponential backoff.** A production system would typically increase the delay between attempts. Kept fixed here to keep the topology easy to reason about.
* **Replay doesn't check downstream health first.** If Postgres is still down, replayed messages will just cycle through the retry loop and land back in the DLQ. A more robust version would health-check before replaying or replay into a quarantine queue for manual review.
* **No horizontal scaling demo.** `storage_writer` and `alerter` run as a single instance each. RabbitMQ's competing-consumers pattern would let you run multiple instances of `storage_writer` for throughput — not implemented here, but the `Qos(10, 0, false)` prefetch setting is already in place to support it.
* **Mock producers, not real services.** Log messages are generated by a weighted-random simulator, not actual application traffic.

## Running the full pipeline

```bash
# 1. Infra
docker compose up -d

# 2. Consumer (storage_writer + alerter)
cd consumer && go mod tidy && go run .

# 3. Producer (separate terminal)
cd producer && go mod tidy && go run .

# 4. Dashboard (separate terminal)
cd dashboard
python3 -m venv venv && source venv/bin/activate
pip install django psycopg2-binary
python manage.py migrate
python manage.py createsuperuser
python manage.py runserver
```

Visit `http://localhost:8000/admin/` to browse logs. Visit `http://localhost:15672` (RabbitMQ management UI, `logs_user` / `logs_pass`) to watch exchanges, queues, and message rates live.
