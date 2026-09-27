# Auction Service — Design

**Date:** 2026-09-27
**Author:** Trần Nguyễn Minh An (leader, team polyrepo submission)
**Status:** Approved by user (design conversation), pending written-spec review before implementation planning

## Context

This is the next service built on the team's polyrepo submission (per the professor's
requirement that each microservice be its own repo and its own independent Spring Boot
project — see `docs/adr/0001-polyrepo-for-team-submission.md`). Already built and verified
running end-to-end: `discovery-server`, `api-gateway`, `common-libs`, `user-service`
(register/login/change-password, JWT, RBAC), `catalog-service` (Category/Product CRUD,
search, discovery, outbox-published domain events).

This sub-project builds the Auction Service (SRS §3.5: Auction Management, Bidding, Auction
Settlement) — the core domain feature of the "e-commerce + auction" marketplace. All
architectural conventions established by `user-service` and `catalog-service` carry forward
unchanged: hexagonal architecture (domain/application/infrastructure/api, zero framework
imports in `domain`), one Postgres database per service (Flyway-migrated), Eureka
registration, routing through `api-gateway`, JWT authentication with per-service
`@RequiresPrivilege` authorization, and the transactional Outbox pattern for reliable Kafka
event publishing.

**Dependency-deferral decision (explicitly discussed and approved by the user,
2026-09-27):** three requirements in SRS §3.5 depend on services or subsystems that do not
exist yet anywhere in the system:
1. Reputation checks (`MIN_REPUTATION_TO_BID`, `MIN_REPUTATION_TO_CREATE_AUCTION`) — no
   reputation/trust system has been built in any service, including `user-service`.
2. Order creation from a completed auction — Commerce Service does not exist.
3. Payment-confirmation tracking for the payment deadline — payment itself is Commerce's
   responsibility, which doesn't exist yet.

For all three, Auction Service builds its own domain logic completely and correctly, and
where the SRS requires interaction with the missing piece, it **only publishes a Kafka
domain event** and takes no further action. This mirrors the placeholder pattern already
used in `catalog-service` (the "product has an order" check on delete was a documented
no-op while Commerce didn't exist).

## Goal of this sub-project

A client, through the gateway, can: create an auction on an existing product (as Seller),
place bids on an active auction (as any authenticated user), and see the auction
automatically transition through its lifecycle and settle with a correctly determined
winner — with the same reliability guarantees (outbox-published Kafka events) and
access-control model as the rest of the system.

## Scope

**In scope:**
- New repo `auction-service`, own Spring Boot project, registered with Eureka, depends on
  `common-libs` (`common-core`, `common-web`, `common-events`, `common-security`)
- **Auction Management:** create (Seller), configure/update (only while `PENDING`), cancel
  (Seller — only while `PENDING`, or `ACTIVE` with zero bids; Admin — anytime,
  `AUCTION.ADMIN_CANCEL`), automatic lifecycle transitions (`PENDING → ACTIVE → ENDED`) via
  a scheduled poller
- **Bidding:** place bid with minimum-increment validation, pessimistic-lock concurrency
  control (correctness under concurrent bids on the same auction), bid history, `Outbid`
  event to the previously-highest bidder, anti-sniping extension (bounded by a new
  `MAX_AUCTION_EXTENSIONS` constant)
- **Settlement:** end processing (lock auction, reject further bids), winner determination
  (highest valid bid; no bids → unsuccessful), `AuctionWon`/`AuctionFailed` event, a
  payment-deadline timer that fires `AuctionPaymentTimeout` unconditionally at the deadline
  (no payment-confirmation signal exists yet to cancel it)
- New domain events added to `common-libs`'s `common-events` module: `AuctionCreated`,
  `AuctionScheduled`, `AuctionStarted`, `BidPlaced`, `Outbid`, `AuctionCancelled`,
  `AuctionEnded`, `AuctionWon`, `AuctionFailed`, `AuctionPaymentTimeout`, `AuctionSettled` —
  published via the same Outbox pattern as `user-service`/`catalog-service`
- New privileges added to `common-libs`'s `common-security` module: `AUCTION.CREATE`,
  `AUCTION.UPDATE`, `AUCTION.CANCEL`, `AUCTION.ADMIN_CANCEL`, `AUCTION.VIEW`,
  `AUCTION.LIST`, `AUCTION.BID`, `AUCTION.VIEW_BID_HISTORY` (this sub-project bumps
  `common-libs`'s version and publishes it; `user-service`/`catalog-service` do not need to
  pick up the new version, since they don't reference auction privileges). `AUCTION.VIEW`,
  `AUCTION.LIST`, and `AUCTION.VIEW_BID_HISTORY` are defined (per the SRS's privilege
  table) but not enforced with `@RequiresPrivilege` — the endpoints they'd gate are public,
  matching how `catalog-service` defines `PRODUCT.VIEW`/`PRODUCT.LIST`/`PRODUCT.SEARCH`
  without gating them
- Gateway routing: `/api/v1/auctions/**` added to `api-gateway`'s route config; write
  endpoints (create/update/cancel/bid) require a valid JWT (checked coarsely at the gateway,
  by privilege inside `auction-service`); read endpoints (list/view/bid-history) public,
  matching Catalog's model

**Out of scope for this sub-project (deferred):**
- Reputation checks — not enforced; `MIN_REPUTATION_TO_BID`/`MIN_REPUTATION_TO_CREATE_AUCTION`
  exist in the SRS but no reputation data exists anywhere to check against
- Actual order creation — Auction Service emits `AuctionWon` with settlement data
  (product, final price, buyer, seller) and stops there; Commerce Service (not built) is the
  intended future consumer
- Real payment-confirmation tracking — see dependency-deferral decision above
- Product-existence validation via a live call to `catalog-service` — `productId` is stored
  as an opaque UUID, the same pattern `catalog-service` uses for `sellerId` (no cross-service
  synchronous calls between business services)
- Auction visibility/restricted-access rules (SRS mentions "public or restricted" without
  further detail) — MVP: all auctions are public
- Inventory reservation on auction create/settle — Fulfillment Service's responsibility
  later; no call is made from this sub-project
- Any change to `user-service`, `catalog-service`, or `discovery-server` beyond the
  `common-libs` version bump and the new gateway route described above

## Repository structure

Following `catalog-service`'s exact package shape:

```
auction-service/
├── pom.xml
├── Dockerfile
├── src/main/java/com/nexus/auction/
│   ├── api/
│   │   ├── AuctionController.java
│   │   ├── BidController.java
│   │   ├── dto/request/, dto/response/
│   │   └── mapper/ (MapStruct)
│   ├── application/
│   │   ├── usecase/          # CreateAuctionUseCase, UpdateAuctionUseCase,
│   │   │                     # CancelAuctionUseCase, AdminCancelAuctionUseCase,
│   │   │                     # PlaceBidUseCase, ListAuctionsUseCase, GetBidHistoryUseCase
│   │   ├── port/out/         # AuctionRepositoryPort, BidRepositoryPort,
│   │   │                     # EventPublisherPort (reused shape from catalog-service)
│   │   └── exception/
│   ├── domain/
│   │   ├── model/            # Auction, Bid, AuctionStatus — plain Java
│   │   └── service/          # BidValidationPolicy, AntiSnipingPolicy
│   └── infrastructure/
│       ├── persistence/      # JPA entities + adapters for Auction, Bid, Outbox
│       ├── messaging/        # OutboxEventPublisherAdapter, OutboxRelayJob
│       ├── scheduling/       # AuctionLifecycleJob, PaymentDeadlineJob
│       └── config/           # SecurityConfig, UseCaseConfig
├── src/main/resources/
│   ├── application.yml
│   └── db/migration/         # V1__create_auctions_table.sql,
│                              # V2__create_bids_table.sql,
│                              # V3__create_outbox_table.sql
└── src/test/java/...
```

## Data model

```sql
auctions (
    id UUID PK,
    product_id UUID NOT NULL,             -- opaque; references catalog-service's Product.id
    seller_id UUID NOT NULL,              -- opaque; references user-service's User.id
    starting_price NUMERIC(12,2) NOT NULL,
    bid_increment NUMERIC(12,2) NOT NULL,
    current_highest_bid NUMERIC(12,2) NULL,
    current_highest_bidder_id UUID NULL,
    status VARCHAR NOT NULL,              -- PENDING | ACTIVE | ENDED | CANCELLED
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,        -- mutable while ACTIVE, via anti-sniping extension
    extension_count INT NOT NULL DEFAULT 0,  -- capped at MAX_AUCTION_EXTENSIONS
    winner_id UUID NULL,
    final_price NUMERIC(12,2) NULL,
    payment_deadline TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
)

bids (
    id UUID PK,
    auction_id UUID NOT NULL REFERENCES auctions(id),
    bidder_id UUID NOT NULL,              -- opaque; references user-service's User.id
    amount NUMERIC(12,2) NOT NULL,
    placed_at TIMESTAMPTZ NOT NULL
)

outbox (
    -- identical shape to user-service/catalog-service's outbox table
    id UUID PK, aggregate_id VARCHAR, event_type VARCHAR, payload TEXT,
    created_at TIMESTAMPTZ, published_at TIMESTAMPTZ NULL
)
```

**Design decision — `current_highest_bid`/`current_highest_bidder_id` denormalized onto
`auctions`:** avoids a `MAX(amount)` query over `bids` on every read (auction list/detail
endpoints are hot, public, unauthenticated). Every bid write updates both `bids` and
`auctions` in the same transaction, under the same row lock — see Data flow.

**Design decision — `MAX_AUCTION_EXTENSIONS = 12`:** the SRS requires anti-sniping
extension but does not specify a cap on total extensions ("enforce limits on auction
extensions according to defined rules" — rule undefined). 12 extensions × 5 minutes
(`ANTI_SNIPING_EXTENSION_MINUTES`) bounds worst-case auction overrun to 1 hour past the
original `end_time`. This is a decision made for this sub-project, not sourced from the
SRS — flagged here for the professor/mentor if grading precision matters.

**Design decision — cancel eligibility:** SRS says cancellation eligibility "shall be
evaluated based on the current auction status and defined business rules" without stating
the rules. This sub-project defines: Seller can cancel while `PENDING`, or while `ACTIVE`
with zero bids recorded; once at least one bid exists, only Admin
(`AUCTION.ADMIN_CANCEL`) can cancel. Rationale: once a bidder has committed to bidding, a
seller unilaterally pulling the auction is a trust/dispute concern the system should not
allow without Admin oversight — same reasoning class as `ALLOW_PRODUCT_DELETE_WITH_ORDER`
in Catalog.

## API

| Method | Path | Privilege | Notes |
|---|---|---|---|
| `POST` | `/api/v1/auctions` | `AUCTION.CREATE` (Seller) | Validates: no other `PENDING`/`ACTIVE` auction for the same `productId` (checked in this service's own table, no call to catalog-service); `start_time < end_time`; duration within `AUCTION_MIN_DURATION_MINUTES`/`AUCTION_MAX_DURATION_HOURS`; seller under `MAX_ACTIVE_AUCTIONS_PER_SELLER` |
| `PUT` | `/api/v1/auctions/{id}` | `AUCTION.UPDATE` | Only while `status = PENDING` |
| `DELETE` | `/api/v1/auctions/{id}` | `AUCTION.CANCEL` (auction's own seller) | 409 if not `PENDING` and not (`ACTIVE` with zero bids) |
| `POST` | `/api/v1/auctions/{id}/admin-cancel` | `AUCTION.ADMIN_CANCEL` (Admin) | Cancellable regardless of status/bid count (except already `ENDED`/`CANCELLED`) |
| `GET` | `/api/v1/auctions/{id}` | public | |
| `GET` | `/api/v1/auctions` | public | Filter (`status`, `sellerId`, `productId`), sort, pagination |
| `POST` | `/api/v1/auctions/{id}/bids` | `AUCTION.BID` | Body: `amount`. Requires `status = ACTIVE` |
| `GET` | `/api/v1/auctions/{id}/bids` | public | Bid history, paginated, newest first |

## Data flow

**Create auction (mirrors `catalog-service`'s create-product flow):**
1. `POST /api/v1/auctions` → gateway (protected path) → routed to `auction-service` via
   Eureka
2. `AuctionController` validates the request shape; `@RequiresPrivilege("AUCTION.CREATE")`
   checks the caller's JWT privileges
3. `CreateAuctionUseCase` (`@Transactional`): validates duration bounds, no conflicting
   active/pending auction for `productId`, seller's active-auction count; creates the
   `Auction` (`status = PENDING`), writes an `AuctionCreated` outbox row — same transaction
4. `OutboxRelayJob` (new instance scoped to `auction-service`'s own outbox table, same
   polling design as the other services) publishes to Kafka topic `auction-events`

**Place bid (the critical path):**
1. `POST /api/v1/auctions/{id}/bids` → gateway → `auction-service`;
   `@RequiresPrivilege("AUCTION.BID")` checks the caller
2. `PlaceBidUseCase` (`@Transactional`): `SELECT ... FOR UPDATE` on the `auctions` row →
   validate `status = ACTIVE` and `amount >= current_highest_bid + bid_increment` (or
   `>= starting_price` if no bids yet) → insert into `bids` → update
   `current_highest_bid`/`current_highest_bidder_id` on `auctions` → if `end_time - now() <
   ANTI_SNIPING_EXTENSION_MINUTES` and `extension_count < MAX_AUCTION_EXTENSIONS`, extend
   `end_time` and increment `extension_count` → write `BidPlaced` outbox row, and an
   `Outbid` outbox row targeting the previous highest bidder if one existed — all in the
   same transaction, released when the transaction commits
3. `OutboxRelayJob` publishes both events to Kafka

**Auction lifecycle (scheduled, no client request involved):**
- `AuctionLifecycleJob` (`@Scheduled(fixedRate = ...)`, e.g. every 5–10s): queries
  `status = PENDING AND start_time <= now()` → transitions to `ACTIVE`, writes
  `AuctionStarted` outbox row (per-row, own transaction, so one slow row doesn't block
  others)
- Same job (or a second one) queries `status = ACTIVE AND end_time <= now()` → locks the
  row, sets `status = ENDED`, determines winner (`current_highest_bidder_id`/
  `current_highest_bid` if present, else none), sets `winner_id`/`final_price`, sets
  `payment_deadline = now() + AUCTION_PAYMENT_DEADLINE_HOURS` if there's a winner, writes
  `AuctionEnded` + (`AuctionWon` or `AuctionFailed`) outbox rows

**Payment deadline (scheduled):**
- `PaymentDeadlineJob` (`@Scheduled`): queries `status = ENDED AND payment_deadline <=
  now() AND` a "timeout not yet emitted" flag → writes `AuctionPaymentTimeout` outbox row,
  marks it emitted (idempotency guard against re-firing on the next poll)

## Error handling

Identical pattern to the established `GlobalExceptionHandler` from `common-web`, reused
as-is. Auction not found → 404; invalid duration/bid-too-low/malformed request → 400;
bid on non-`ACTIVE` auction, or cancel not eligible → 409; caller is not the auction's
seller on an update/cancel → 403.

## Testing strategy

Same shape as `catalog-service`: unit tests for domain rules (`BidValidationPolicy`,
`AntiSnipingPolicy`) with no Spring context; Testcontainers-backed integration tests for
the repository adapters and the outbox write; a concurrency-specific integration test that
fires many bids at the same auction from parallel threads and asserts exactly the highest
valid bid wins with no lost updates; tests for both scheduled jobs (lifecycle transitions,
payment-deadline emission, idempotency of the deadline flag); reuse of the existing
`GlobalExceptionHandlerTest` coverage without modification.

## Key decisions carried forward unchanged

ADR-0001 (polyrepo for team submission) applies identically — no new ADR needed. The
data-modeling and scope decisions specific to this service (denormalized highest-bid
fields, `MAX_AUCTION_EXTENSIONS`, cancel-eligibility rule, dependency-deferral via
events-only) are documented inline above rather than as separate ADRs, matching how
`catalog-service`'s price-on-`Sku` decision was handled.

## Open questions / risks carried forward

- The overall 6-service scope's feasibility given the team's actual size/timeline is still
  unconfirmed with the professor (carried from sub-project #1 and #2's specs).
- `MAX_AUCTION_EXTENSIONS = 12` and the cancel-eligibility-after-first-bid rule are both
  decisions made for this sub-project, not sourced from the SRS — worth a quick sanity
  check with the professor/mentor if grading rewards SRS-literal fidelity.
- When Commerce Service is eventually built, it will need to consume `AuctionWon` from the
  `auction-events` Kafka topic to create orders — no code changes to `auction-service`
  should be needed for that integration, only a new consumer on Commerce's side.
