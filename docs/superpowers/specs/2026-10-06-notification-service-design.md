# Notification Service — Design

**Date:** 2026-10-06
**Author:** Trần Nguyễn Minh An (leader, team polyrepo submission)
**Status:** Approved by user (design conversation), pending written-spec review before implementation planning

## Context

The team's polyrepo already has three business services publishing domain events via the
Outbox pattern (`user-events` from `user-service`, `catalog-events` from `catalog-service`,
`auction-events` from `auction-service`), but no consumer has ever existed anywhere in the
system — confirmed by grepping for `@KafkaListener` across all repos (zero results). The
Kafka presentation given to the professor explicitly stated this gap and promised a
Notification Service as the next step. This sub-project closes that gap.

This is next-sprint work, built after the presentation, under the same time constraints as
a capstone project (small team, limited remaining weeks). The design favors the smallest
scope that is still a genuine, defensible professional pattern — not a toy shortcut.

## Goal of this sub-project

A new service consumes all three existing Kafka topics, durably records every event it
receives (for system-wide traceability), and derives a human-readable, per-user notification
for a known subset of event types. Two read APIs expose this: a personal "my notifications"
inbox, and an audit view over every event received, regardless of whether a notification was
derived from it.

## Scope

**In scope:**
- New repo `notification-service`, own Spring Boot project (Java 21, Spring Boot 3.3.4,
  same stack as the other three services), registered with Eureka, port `8084` (next
  available port per existing convention), depends on `common-libs`
  (`common-core`, `common-web`, `common-security`, `common-events`)
- Kafka consumer: three `@KafkaListener` methods, one per topic (`user-events`,
  `catalog-events`, `auction-events`) — not a single topic-pattern listener (see
  "Approaches considered" below)
- Consumer group id `notification-service`, so that running multiple instances later does
  not cause duplicate processing (Kafka's native partition-per-consumer guarantee)
- One Postgres database, `notification_db` (new container `postgres-notification`, port
  `5435`), Flyway-migrated, following the same one-DB-per-service convention as the rest of
  the system
- Idempotent recording: every event's `eventId` (already present on every `DomainEvent`
  subclass) is stored as a unique column. A re-delivered event (Kafka's at-least-once
  guarantee can redeliver) is detected via a unique-constraint violation and treated as a
  no-op, not an error.
- Message/recipient resolution for a known subset of five event types:
  `UserRegisteredEvent`, `BidPlacedEvent`, `OutbidEvent`, `AuctionWonEvent`,
  `AuctionSettledEvent`. Every other event type (the remaining twelve: Product/Category
  events, and the rest of the Auction lifecycle events) is still recorded in full — raw
  payload preserved — but with `recipient_user_id` and `message` left `NULL`. No attempt is
  made to map all seventeen event types; unmapped types are audit-only by design, not a
  missing feature.
- Two read endpoints:
  - `GET /api/v1/notifications/me` — JWT-authenticated, returns rows where
    `recipient_user_id` matches the caller's JWT subject. This is the per-user inbox.
  - `GET /api/v1/notifications` — gated by a new privilege `NOTIFICATION.AUDIT`, returns all
    rows regardless of recipient, filterable by `eventType` and `aggregateId` query params.
    This is the traceability/audit view.
  - `PATCH /api/v1/notifications/{id}/read` — JWT-authenticated, marks a notification the
    caller owns as read. Returns 403 (via `ForbiddenException`) if the caller does not own
    the row.
- New privilege `NOTIFICATION.AUDIT`, seeded in `user-service`'s migrations (next sequential
  version, `V7__seed_notification_privileges.sql`), granted to the `ADMIN` role only — this
  sub-project touches `user-service`, not just the new repo.
- Gateway routing: `api-gateway`'s `application.yml` gets a new route,
  `Path=/api/v1/notifications/**` → `lb://notification-service`.

**Out of scope (explicitly deferred):**
- Email, SMS, or push delivery channels — only the "in-app" channel (DB row + read API)
  is built. This is a real, standard channel on its own, not a placeholder for a missing
  feature; adding other channels later does not require reworking this service, only adding
  new delivery workers downstream of the same recorded data.
- Dead-letter queue for malformed/unprocessable messages — logged and dropped instead (see
  Error Handling). Noted as a future-work item, not implemented now.
- Mapping all seventeen event types to human-readable messages — only the five listed above.
- WebSocket/real-time push of new notifications to a connected client — the API is
  pull-only (`GET`), no server push.
- Any changes to how `user-service`/`catalog-service`/`auction-service` publish events —
  this sub-project only adds a consumer, the existing Outbox/producer side is untouched.

## Architecture

Same hexagonal layering as every other service in this system:

```
notification-service/src/main/java/com/nexus/notification/
├── api/
│   ├── NotificationController.java        # the 3 endpoints above
│   └── dto/
├── application/
│   ├── usecase/
│   │   ├── RecordNotificationUseCase.java # called by all 3 listeners
│   │   ├── ListMyNotificationsUseCase.java
│   │   ├── ListAllNotificationsUseCase.java (audit)
│   │   └── MarkNotificationReadUseCase.java
│   ├── port/out/NotificationRepositoryPort.java
│   └── exception/
├── domain/
│   ├── model/Notification.java            # plain Java
│   └── service/NotificationMessageResolver.java  # maps the 5 known event types
└── infrastructure/
    ├── persistence/        # JPA entity + adapter implementing NotificationRepositoryPort
    ├── messaging/
    │   ├── UserEventsListener.java         # @KafkaListener(topics = "user-events")
    │   ├── CatalogEventsListener.java      # @KafkaListener(topics = "catalog-events")
    │   └── AuctionEventsListener.java      # @KafkaListener(topics = "auction-events")
    └── config/SecurityConfig.java, UseCaseConfig.java
```

`NotificationMessageResolver` lives in `domain/service` (zero framework imports, pure
logic) — consistent with how `BidValidationPolicy`/`AntiSnipingPolicy` are structured in
`auction-service`. It takes the generic envelope fields plus the raw JSON payload, and for
the five known event types deserializes the payload into the concrete `common-events` class
to read type-specific fields (e.g., `AuctionWonEvent.winnerId()`, `AuctionWonEvent.finalPrice()`).
For anything else it returns `(recipientUserId=null, message=null)`.

## Data model

```sql
-- V1__create_notifications_table.sql (notification-service's own migration)
CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id VARCHAR(255) NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(255),
    recipient_user_id VARCHAR(255),
    message TEXT,
    payload TEXT NOT NULL,
    is_read BOOLEAN NOT NULL DEFAULT false,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_notifications_recipient ON notifications (recipient_user_id);
CREATE INDEX idx_notifications_event_type ON notifications (event_type);
```

`event_id` is the idempotency key. `recipient_user_id`/`message` are nullable by design —
a `NULL` recipient means "audit-only, not shown in anyone's personal inbox."

## Data flow (example: a bidder wins an auction)

```
auction-service: OutboxRelayJob → kafkaTemplate.send("auction-events", eventId, jsonPayload)
        │
        ▼
notification-service: AuctionEventsListener.onMessage(record)
        │  parse payload as JsonNode first — read only the 4 fields every DomainEvent has
        │  (eventId, eventType, aggregateId, occurredAt); the full payload string is kept
        │  as-is for the `payload` column regardless of event type
        ▼
   RecordNotificationUseCase.record(envelope, rawPayloadJson)
        ├─ NotificationMessageResolver.resolve("AuctionWonEvent", rawPayloadJson)
        │     → recognized → deserialize into AuctionWonEvent → read winnerId, finalPrice
        │     → returns (recipientUserId = winnerId, message = "Bạn đã thắng phiên đấu giá ...")
        └─ notificationRepositoryPort.save(Notification{... recipientUserId, message ...})
              → INSERT INTO notifications (event_id UNIQUE constraint enforced here)
```

If the same `AuctionWonEvent` is redelivered by Kafka, the second `INSERT` violates the
`event_id` unique constraint; the adapter catches `DataIntegrityViolationException` and
treats it as a successful no-op (logged at INFO, not ERROR).

## Error handling

| Situation | Handling |
|---|---|
| Duplicate event (Kafka at-least-once redelivery) | Catch `DataIntegrityViolationException` on the unique `event_id` constraint, log INFO, no-op |
| Payload is not valid JSON / missing expected fields | Catch broadly in the listener, log ERROR with the raw record, continue (does not throw out of the listener method, so other records keep being processed) |
| Database temporarily unreachable | No custom retry logic; rely on Spring Kafka's default consumer error-handling/backoff. Explicitly not building a DLQ for this sub-project. |
| Unknown `eventType` (not one of the 5 mapped) | Not an error — `NotificationMessageResolver` returns `(null, null)`, row is still recorded with full payload |

## Testing (matches existing project convention)

- `NotificationMessageResolverTest` — plain JUnit, no Spring context. Covers all 5 mapped
  event types plus one unmapped type, asserting correct `(recipientUserId, message)` or
  `(null, null)`.
- `RecordNotificationUseCaseIntegrationTest` — Testcontainers Postgres. Asserts a normal
  insert, and asserts that inserting the same `eventId` twice results in exactly one row
  (idempotency).
- `NotificationListenerIntegrationTest` — Testcontainers Kafka + Postgres, mirrors the
  existing `OutboxRelayJobTest` pattern in `catalog-service`/`auction-service`: publish a
  test message to a test topic, assert a row appears.
- Controller tests for the 3 endpoints, reusing `GlobalExceptionHandlerTest` unmodified like
  every other service.

## Cross-repo changes required

This sub-project is not self-contained in the new repo alone:

1. `infra/docker-compose.yml` — add `postgres-notification` (port 5435) and
   `notification-service` (port 8084) services.
2. `api-gateway/src/main/resources/application.yml` — add the `/api/v1/notifications/**`
   route.
3. `user-service` — new migration `V7__seed_notification_privileges.sql` adding
   `NOTIFICATION.AUDIT` and granting it to `ADMIN`.

## Approaches considered

**Chosen — one `@KafkaListener` per topic, single shared table.** Matches the explicit,
one-thing-per-class style already used throughout the codebase (e.g., separate
`AuctionLifecycleJob`/`PaymentDeadlineJob` rather than one combined job). Easy to reason
about and debug per topic.

**Rejected — single listener with `topicPattern` regex matching all three topics.** Less
boilerplate, but less explicit about which topic produced which log line, and harder to
apply topic-specific handling later if needed. Inconsistent with the codebase's existing
preference for explicit over clever.

**Rejected — two separate tables (`event_log` generic + `notifications` curated).** This is
closer to how large companies actually separate "audit trail" from "user-facing
notification" (in practice the audit role is often just Kafka's own topic retention, not a
service-owned copy at all). Rejected for this sub-project as unnecessary schema/API
duplication given the one-table-with-nullable-recipient design already satisfies both needs
at this scope. Could be revisited if the audit requirements grow significantly.

## Future work (explicitly not building now)

- Additional delivery channels (email/SMS/push) as separate workers reading already-recorded
  notifications — the current design does not need to change to support this later.
- Dead-letter queue for malformed messages.
- Mapping the remaining twelve event types to human-readable messages, if a real need
  surfaces (e.g., sellers wanting to see "your product was created" notifications).
