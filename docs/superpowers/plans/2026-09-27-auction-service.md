# Auction Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Auction Service — a new `auction-service` microservice that lets sellers create auctions on existing Catalog products, lets buyers bid, and automatically runs the auction lifecycle (start → bidding → settlement) with events published for future consumption by Commerce/Notification.

**Architecture:** Hexagonal architecture identical to `catalog-service` (domain/application/infrastructure/api, zero framework imports in `domain`), one Postgres database, Eureka registration, routing through `api-gateway`, JWT + `@RequiresPrivilege` authorization, transactional Outbox pattern for Kafka publishing. Bid concurrency uses Postgres pessimistic row locking (`SELECT ... FOR UPDATE` via JPA `@Lock(PESSIMISTIC_WRITE)`). Lifecycle transitions and payment-deadline enforcement run via `@Scheduled` polling jobs, the same shape as `OutboxRelayJob`.

**Tech Stack:** Java 21, Spring Boot 3.3.4, Spring Cloud 2023.0.3 (Eureka client), Spring Data JPA + Postgres 16 + Flyway, Spring Kafka, Spring Security (stateless JWT), MapStruct, JUnit 5 + AssertJ + Mockito + Testcontainers (Postgres, Kafka).

**Spec:** `docs/superpowers/specs/2026-09-27-auction-service-design.md` (this `infra` repo)

## Global Constraints

- `AUCTION_MIN_DURATION_MINUTES = 60`, `AUCTION_MAX_DURATION_HOURS = 168` — auction duration bounds, enforced at creation.
- `MAX_ACTIVE_AUCTIONS_PER_SELLER = 5` — a seller cannot have more than 5 `PENDING`/`ACTIVE` auctions at once.
- `ANTI_SNIPING_EXTENSION_MINUTES = 5` — an in-window bid pushes `end_time` out by 5 minutes.
- `MAX_AUCTION_EXTENSIONS = 12` — hard cap on total extensions per auction (this sub-project's own decision, not from the SRS — see spec).
- `AUCTION_PAYMENT_DEADLINE_HOURS = 24` — winner's payment window; `AuctionPaymentTimeout` fires unconditionally at the deadline (no payment-confirmation signal exists yet — see spec's dependency-deferral decision).
- Anti-sniping is always on (no feature toggle infra exists anywhere in this codebase for it — same reasoning as `CategoryDepthPolicy`'s hardcoded max depth in `catalog-service`).
- `productId`/`sellerId`/bidder ids are opaque `String` UUIDs — never call `catalog-service` or `user-service` synchronously to validate them (matches `catalog-service`'s established pattern).
- Package root: `com.nexus.auction`. Privileges: `AUCTION.CREATE`, `AUCTION.UPDATE`, `AUCTION.CANCEL`, `AUCTION.ADMIN_CANCEL`, `AUCTION.BID` are enforced via `@RequiresPrivilege`; `AUCTION.VIEW`/`AUCTION.LIST`/`AUCTION.VIEW_BID_HISTORY` are defined (seeded) but not gated — their endpoints are public, matching `PRODUCT.VIEW`/`LIST`/`SEARCH`.

## Review Focus

- A bid placed at exactly `current_highest_bid + bid_increment` (not strictly greater) must be **accepted** — an off-by-one here silently locks out the minimum valid bid. Covered in Task 5's `BidValidationPolicy` tests and Task 13's use case tests.
- Two bids fired concurrently at the same auction, where the second is only valid relative to the first committing first, must not lose either decision (classic lost-update under concurrency) — covered by Task 13's parallel-thread integration test.
- Bidding on an auction that is `PENDING` (not yet started), `ENDED`, or `CANCELLED` must be rejected with 409, never silently accepted or a 500 — covered in Task 5 and Task 13.
- An auction sitting inside the anti-sniping window on many consecutive bids must stop extending exactly at `MAX_AUCTION_EXTENSIONS` and never exceed it — covered in Task 6's `AntiSnipingPolicy` tests.
- `AuctionLifecycleJob`/`PaymentDeadlineJob` firing twice on overlapping poll ticks (a slow run overlapping the next scheduled tick) must not double-transition an auction to `ENDED` or emit a duplicate `AuctionEnded`/`AuctionPaymentTimeout` event — covered in Task 16 and Task 17 via an idempotency-guard test (re-invoking the job a second time against already-settled/already-flagged rows and asserting no duplicate event row is written).

---

### Task 1: Bootstrap the `auction-service` repository

**Files:**
- Create (new repo, cloned as a sibling of `catalog-service` etc. under `C:\FPT`): `auction-service/pom.xml`, `auction-service/Dockerfile`, `auction-service/src/main/resources/application.yml`, `auction-service/src/main/java/com/nexus/auction/AuctionServiceApplication.java`, `auction-service/.github/workflows/ci.yml`, `auction-service/.gitignore`
- Test: `auction-service/src/test/java/com/nexus/auction/AuctionServiceApplicationTests.java`

**Interfaces:**
- Produces: a bootable Spring Boot app on port `8083`, registered with Eureka, with Flyway/JPA/Kafka/Security wired (empty `db/migration` for now — later tasks add migrations here).

- [ ] **Step 1: Create the GitHub repo and local clone**

```bash
gh repo create antran19/auction-service --public --description "Auction Service for Project Nexus"
git clone https://github.com/antran19/auction-service.git /c/FPT/auction-service
```

- [ ] **Step 2: Add `.gitignore`, `pom.xml`, `Dockerfile`, `application.yml`**

`.gitignore`:
```
target/
*.class
.idea/
*.iml
```

`pom.xml` (copy of `catalog-service/pom.xml` with `artifactId`/`name` changed to `auction-service` — same parent-less structure, same dependency set: `common-core`, `common-web`, `common-events`, `common-security` at `${common-libs.version}`, `spring-boot-starter-web`, `spring-boot-starter-data-jpa`, `spring-boot-starter-actuator`, `spring-boot-starter-validation`, `spring-boot-starter-security`, `spring-cloud-starter-netflix-eureka-client`, `spring-kafka`, `postgresql` (runtime), `flyway-core`, `flyway-database-postgresql`, `mapstruct` + its annotation processor, test deps `spring-boot-starter-test`, `testcontainers` (`junit-jupiter`, `postgresql`, `kafka`) — identical version properties: `java.version=21`, `spring-boot.version=3.3.4`, `spring-cloud.version=2023.0.3`, `mapstruct.version=1.6.2`, `testcontainers.version=1.20.1`, `common-libs.version=1.1.0`). Use `common-libs.version=1.1.0` (not `1.0.0`) — Task 2 bumps `common-libs` to `1.1.0` to add the Auction events, and this service is the first consumer of that version.

`Dockerfile`:
```dockerfile
FROM eclipse-temurin:21-jre
COPY target/auction-service-0.1.0-SNAPSHOT.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

`src/main/resources/application.yml`:
```yaml
server:
  port: 8083

spring:
  application:
    name: auction-service
  datasource:
    url: jdbc:postgresql://localhost:5434/auction_db
    username: nexus
    password: nexus
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
  flyway:
    enabled: true
    locations: classpath:db/migration
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

eureka:
  client:
    service-url:
      defaultZone: http://localhost:8761/eureka
  instance:
    prefer-ip-address: true

nexus:
  security:
    jwt:
      secret: "dev-only-secret-key-change-in-production-min-256-bits-long!!"
      expiration-minutes: 60

management:
  endpoints:
    web:
      exposure:
        include: health,info
```

`src/main/java/com/nexus/auction/AuctionServiceApplication.java`:
```java
package com.nexus.auction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.nexus")
@EnableDiscoveryClient
@EnableScheduling
public class AuctionServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuctionServiceApplication.class, args);
    }
}
```

- [ ] **Step 3: Write the context-loads test**

`src/test/java/com/nexus/auction/AuctionServiceApplicationTests.java`:
```java
package com.nexus.auction;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AuctionServiceApplicationTests {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("auction_db")
            .withUsername("nexus")
            .withPassword("nexus");

    @DynamicPropertySource
    static void configureDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eureka.client.enabled", () -> "false");
    }

    @Test
    void contextLoads() {
    }
}
```

- [ ] **Step 4: Run the test to verify it fails (no `common-libs` 1.1.0 yet)**

Run: `cd /c/FPT/auction-service && mvn -B clean verify`
Expected: FAIL — Maven cannot resolve `com.nexus:common-core:1.1.0` (doesn't exist until Task 2).

- [ ] **Step 5: Commit (test will pass once Task 2 publishes `common-libs` 1.1.0)**

```bash
git add pom.xml Dockerfile .gitignore src/main/resources/application.yml \
  src/main/java/com/nexus/auction/AuctionServiceApplication.java \
  src/test/java/com/nexus/auction/AuctionServiceApplicationTests.java
git commit -m "feat: bootstrap auction-service Spring Boot project"
```

Do not push yet — Step 2's `mvn verify` will still fail until Task 2 lands `common-libs` 1.1.0 locally. Push at the end of Task 2 once the build is green.

---

### Task 2: `common-libs` — add Auction domain events, bump to 1.1.0

**Files:**
- Create (in `C:\FPT\common-libs`): `common-events/src/main/java/com/nexus/common/events/AuctionCreatedEvent.java`, `AuctionScheduledEvent.java`, `AuctionStartedEvent.java`, `BidPlacedEvent.java`, `OutbidEvent.java`, `AuctionCancelledEvent.java`, `AuctionEndedEvent.java`, `AuctionWonEvent.java`, `AuctionFailedEvent.java`, `AuctionPaymentTimeoutEvent.java`, `AuctionSettledEvent.java`
- Modify: `common-libs/pom.xml:<version>` and the 4 module POMs' inherited version (root `pom.xml` only — modules inherit it), `1.0.0` → `1.1.0`

**Interfaces:**
- Consumes: `DomainEvent` (existing base class: `protected DomainEvent(String eventType, String aggregateId)` and the `@JsonCreator` 4-arg reconstruction constructor — see `common-events/src/main/java/com/nexus/common/events/DomainEvent.java`).
- Produces: 11 event classes, each following `ProductCreatedEvent`'s exact two-constructor shape (a domain constructor calling `super(eventType, aggregateId)`, plus a `@JsonCreator` constructor for deserialization), for `auction-service`'s later tasks to construct and for future Commerce/Notification consumers to deserialize off Kafka.

- [ ] **Step 1: Write the 11 event classes**

All 11 follow the same shape as `ProductCreatedEvent`/`ProductStatusChangedEvent`. `aggregateId` is always the auction's id.

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class AuctionCreatedEvent extends DomainEvent {

    private final String auctionId;
    private final String productId;
    private final String sellerId;

    public AuctionCreatedEvent(String auctionId, String productId, String sellerId) {
        super("AuctionCreated", auctionId);
        this.auctionId = auctionId;
        this.productId = productId;
        this.sellerId = sellerId;
    }

    @JsonCreator
    public AuctionCreatedEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId,
            @JsonProperty("productId") String productId,
            @JsonProperty("sellerId") String sellerId) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
        this.productId = productId;
        this.sellerId = sellerId;
    }

    public String getAuctionId() { return auctionId; }
    public String getProductId() { return productId; }
    public String getSellerId() { return sellerId; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class AuctionScheduledEvent extends DomainEvent {

    private final String auctionId;
    private final Instant startTime;
    private final Instant endTime;

    public AuctionScheduledEvent(String auctionId, Instant startTime, Instant endTime) {
        super("AuctionScheduled", auctionId);
        this.auctionId = auctionId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @JsonCreator
    public AuctionScheduledEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId,
            @JsonProperty("startTime") Instant startTime,
            @JsonProperty("endTime") Instant endTime) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public String getAuctionId() { return auctionId; }
    public Instant getStartTime() { return startTime; }
    public Instant getEndTime() { return endTime; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class AuctionStartedEvent extends DomainEvent {

    private final String auctionId;

    public AuctionStartedEvent(String auctionId) {
        super("AuctionStarted", auctionId);
        this.auctionId = auctionId;
    }

    @JsonCreator
    public AuctionStartedEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
    }

    public String getAuctionId() { return auctionId; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public class BidPlacedEvent extends DomainEvent {

    private final String auctionId;
    private final String bidderId;
    private final BigDecimal amount;

    public BidPlacedEvent(String auctionId, String bidderId, BigDecimal amount) {
        super("BidPlaced", auctionId);
        this.auctionId = auctionId;
        this.bidderId = bidderId;
        this.amount = amount;
    }

    @JsonCreator
    public BidPlacedEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId,
            @JsonProperty("bidderId") String bidderId,
            @JsonProperty("amount") BigDecimal amount) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
        this.bidderId = bidderId;
        this.amount = amount;
    }

    public String getAuctionId() { return auctionId; }
    public String getBidderId() { return bidderId; }
    public BigDecimal getAmount() { return amount; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public class OutbidEvent extends DomainEvent {

    private final String auctionId;
    private final String outbidBidderId;
    private final BigDecimal newHighestBid;

    public OutbidEvent(String auctionId, String outbidBidderId, BigDecimal newHighestBid) {
        super("Outbid", auctionId);
        this.auctionId = auctionId;
        this.outbidBidderId = outbidBidderId;
        this.newHighestBid = newHighestBid;
    }

    @JsonCreator
    public OutbidEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId,
            @JsonProperty("outbidBidderId") String outbidBidderId,
            @JsonProperty("newHighestBid") BigDecimal newHighestBid) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
        this.outbidBidderId = outbidBidderId;
        this.newHighestBid = newHighestBid;
    }

    public String getAuctionId() { return auctionId; }
    public String getOutbidBidderId() { return outbidBidderId; }
    public BigDecimal getNewHighestBid() { return newHighestBid; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class AuctionCancelledEvent extends DomainEvent {

    private final String auctionId;
    private final String cancelledBy;

    public AuctionCancelledEvent(String auctionId, String cancelledBy) {
        super("AuctionCancelled", auctionId);
        this.auctionId = auctionId;
        this.cancelledBy = cancelledBy;
    }

    @JsonCreator
    public AuctionCancelledEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId,
            @JsonProperty("cancelledBy") String cancelledBy) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
        this.cancelledBy = cancelledBy;
    }

    public String getAuctionId() { return auctionId; }
    public String getCancelledBy() { return cancelledBy; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class AuctionEndedEvent extends DomainEvent {

    private final String auctionId;

    public AuctionEndedEvent(String auctionId) {
        super("AuctionEnded", auctionId);
        this.auctionId = auctionId;
    }

    @JsonCreator
    public AuctionEndedEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
    }

    public String getAuctionId() { return auctionId; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public class AuctionWonEvent extends DomainEvent {

    private final String auctionId;
    private final String productId;
    private final String sellerId;
    private final String winnerId;
    private final BigDecimal finalPrice;

    public AuctionWonEvent(String auctionId, String productId, String sellerId, String winnerId, BigDecimal finalPrice) {
        super("AuctionWon", auctionId);
        this.auctionId = auctionId;
        this.productId = productId;
        this.sellerId = sellerId;
        this.winnerId = winnerId;
        this.finalPrice = finalPrice;
    }

    @JsonCreator
    public AuctionWonEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId,
            @JsonProperty("productId") String productId,
            @JsonProperty("sellerId") String sellerId,
            @JsonProperty("winnerId") String winnerId,
            @JsonProperty("finalPrice") BigDecimal finalPrice) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
        this.productId = productId;
        this.sellerId = sellerId;
        this.winnerId = winnerId;
        this.finalPrice = finalPrice;
    }

    public String getAuctionId() { return auctionId; }
    public String getProductId() { return productId; }
    public String getSellerId() { return sellerId; }
    public String getWinnerId() { return winnerId; }
    public BigDecimal getFinalPrice() { return finalPrice; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class AuctionFailedEvent extends DomainEvent {

    private final String auctionId;

    public AuctionFailedEvent(String auctionId) {
        super("AuctionFailed", auctionId);
        this.auctionId = auctionId;
    }

    @JsonCreator
    public AuctionFailedEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
    }

    public String getAuctionId() { return auctionId; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class AuctionPaymentTimeoutEvent extends DomainEvent {

    private final String auctionId;
    private final String winnerId;

    public AuctionPaymentTimeoutEvent(String auctionId, String winnerId) {
        super("AuctionPaymentTimeout", auctionId);
        this.auctionId = auctionId;
        this.winnerId = winnerId;
    }

    @JsonCreator
    public AuctionPaymentTimeoutEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId,
            @JsonProperty("winnerId") String winnerId) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
        this.winnerId = winnerId;
    }

    public String getAuctionId() { return auctionId; }
    public String getWinnerId() { return winnerId; }
}
```

```java
package com.nexus.common.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public class AuctionSettledEvent extends DomainEvent {

    private final String auctionId;
    private final String winnerId;
    private final BigDecimal finalPrice;
    private final Instant settledAt;

    public AuctionSettledEvent(String auctionId, String winnerId, BigDecimal finalPrice, Instant settledAt) {
        super("AuctionSettled", auctionId);
        this.auctionId = auctionId;
        this.winnerId = winnerId;
        this.finalPrice = finalPrice;
        this.settledAt = settledAt;
    }

    @JsonCreator
    public AuctionSettledEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("aggregateId") String aggregateId,
            @JsonProperty("auctionId") String auctionId,
            @JsonProperty("winnerId") String winnerId,
            @JsonProperty("finalPrice") BigDecimal finalPrice,
            @JsonProperty("settledAt") Instant settledAt) {
        super(eventId, eventType, occurredAt, aggregateId);
        this.auctionId = auctionId;
        this.winnerId = winnerId;
        this.finalPrice = finalPrice;
        this.settledAt = settledAt;
    }

    public String getAuctionId() { return auctionId; }
    public String getWinnerId() { return winnerId; }
    public BigDecimal getFinalPrice() { return finalPrice; }
    public Instant getSettledAt() { return settledAt; }
}
```

- [ ] **Step 2: Bump the version and install locally**

In `common-libs/pom.xml`, change `<version>1.0.0</version>` to `<version>1.1.0</version>` (root POM; the 4 modules inherit it — no per-module version to edit).

Run: `cd /c/FPT/common-libs && mvn -q clean install`
Expected: `BUILD SUCCESS`, and `~/.m2/repository/com/nexus/common-events/1.1.0/` now exists.

- [ ] **Step 3: Verify `auction-service` now resolves and builds**

Run: `cd /c/FPT/auction-service && mvn -B clean verify`
Expected: PASS (Task 1's `contextLoads` test now succeeds — `common-events:1.1.0` resolves from the local `~/.m2` cache).

- [ ] **Step 4: Commit and push both repos**

```bash
cd /c/FPT/common-libs
git add pom.xml common-events/src/main/java/com/nexus/common/events/Auction*.java \
  common-events/src/main/java/com/nexus/common/events/BidPlacedEvent.java \
  common-events/src/main/java/com/nexus/common/events/OutbidEvent.java
git commit -m "feat: add Auction domain events; bump to 1.1.0"
git push origin main

cd /c/FPT/auction-service
git push origin main
```

Pushing `common-libs`'s `main` triggers its CI (`publish.yml`), which publishes `1.1.0` to GitHub Packages — needed so CI on `auction-service` (and anyone without a local `mvn install` of `common-libs`) can resolve it too.

---

### Task 3: Domain model — `AuctionStatus` and `Auction`

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/domain/model/AuctionStatus.java`, `auction-service/src/main/java/com/nexus/auction/domain/model/Auction.java`
- Test: `auction-service/src/test/java/com/nexus/auction/domain/model/AuctionTest.java`

**Interfaces:**
- Produces: `Auction` (immutable, `Product`-style `withX` transition methods), `AuctionStatus` enum — consumed by every later use case, policy, and persistence adapter task.

- [ ] **Step 1: Write the failing test**

```java
package com.nexus.auction.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

class AuctionTest {

    @Test
    void create_startsInPendingWithNoBidsAndZeroExtensions() {
        Instant start = Instant.now().plus(1, ChronoUnit.HOURS);
        Instant end = start.plus(2, ChronoUnit.HOURS);

        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"),
                new BigDecimal("10.00"), start, end);

        assertThat(auction.getStatus()).isEqualTo(AuctionStatus.PENDING);
        assertThat(auction.getCurrentHighestBid()).isNull();
        assertThat(auction.getCurrentHighestBidderId()).isNull();
        assertThat(auction.getExtensionCount()).isZero();
        assertThat(auction.getWinnerId()).isNull();
        assertThat(auction.getId()).isNotBlank();
    }

    @Test
    void withStatus_returnsNewInstanceWithUpdatedStatus() {
        Auction pending = Auction.create("product-1", "seller-1", new BigDecimal("100.00"),
                new BigDecimal("10.00"), Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS));

        Auction active = pending.withStatus(AuctionStatus.ACTIVE);

        assertThat(active.getStatus()).isEqualTo(AuctionStatus.ACTIVE);
        assertThat(pending.getStatus()).isEqualTo(AuctionStatus.PENDING);
        assertThat(active.getId()).isEqualTo(pending.getId());
    }

    @Test
    void withBid_updatesHighestBidAndBidder() {
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"),
                new BigDecimal("10.00"), Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS));

        Auction bidded = auction.withBid("bidder-1", new BigDecimal("100.00"));

        assertThat(bidded.getCurrentHighestBid()).isEqualByComparingTo("100.00");
        assertThat(bidded.getCurrentHighestBidderId()).isEqualTo("bidder-1");
    }

    @Test
    void withExtendedEndTime_pushesEndTimeAndIncrementsExtensionCount() {
        Instant originalEnd = Instant.now().plus(1, ChronoUnit.HOURS);
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"),
                new BigDecimal("10.00"), Instant.now(), originalEnd);
        Instant newEnd = originalEnd.plus(5, ChronoUnit.MINUTES);

        Auction extended = auction.withExtendedEndTime(newEnd);

        assertThat(extended.getEndTime()).isEqualTo(newEnd);
        assertThat(extended.getExtensionCount()).isEqualTo(1);
    }

    @Test
    void withSettlement_setsWinnerFinalPriceAndEndedStatus() {
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"),
                new BigDecimal("10.00"), Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS));
        Instant deadline = Instant.now().plus(24, ChronoUnit.HOURS);

        Auction settled = auction.withSettlement("bidder-1", new BigDecimal("150.00"), deadline);

        assertThat(settled.getStatus()).isEqualTo(AuctionStatus.ENDED);
        assertThat(settled.getWinnerId()).isEqualTo("bidder-1");
        assertThat(settled.getFinalPrice()).isEqualByComparingTo("150.00");
        assertThat(settled.getPaymentDeadline()).isEqualTo(deadline);
    }

    @Test
    void withPaymentTimeoutEmitted_setsFlag() {
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"),
                new BigDecimal("10.00"), Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS));

        Auction flagged = auction.withPaymentTimeoutEmitted();

        assertThat(flagged.isPaymentTimeoutEmitted()).isTrue();
        assertThat(auction.isPaymentTimeoutEmitted()).isFalse();
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/FPT/auction-service && mvn -Dtest=AuctionTest test`
Expected: FAIL with "cannot find symbol: class Auction" (and `AuctionStatus`).

- [ ] **Step 3: Write `AuctionStatus`**

```java
package com.nexus.auction.domain.model;

public enum AuctionStatus {
    PENDING, ACTIVE, ENDED, CANCELLED
}
```

- [ ] **Step 4: Write `Auction`**

```java
package com.nexus.auction.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class Auction {

    private final String id;
    private final String productId;
    private final String sellerId;
    private final BigDecimal startingPrice;
    private final BigDecimal bidIncrement;
    private final BigDecimal currentHighestBid;
    private final String currentHighestBidderId;
    private final AuctionStatus status;
    private final Instant startTime;
    private final Instant endTime;
    private final int extensionCount;
    private final String winnerId;
    private final BigDecimal finalPrice;
    private final Instant paymentDeadline;
    private final boolean paymentTimeoutEmitted;
    private final Instant createdAt;
    private final Instant updatedAt;

    private Auction(String id, String productId, String sellerId, BigDecimal startingPrice,
                     BigDecimal bidIncrement, BigDecimal currentHighestBid, String currentHighestBidderId,
                     AuctionStatus status, Instant startTime, Instant endTime, int extensionCount,
                     String winnerId, BigDecimal finalPrice, Instant paymentDeadline,
                     boolean paymentTimeoutEmitted, Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.productId = productId;
        this.sellerId = sellerId;
        this.startingPrice = startingPrice;
        this.bidIncrement = bidIncrement;
        this.currentHighestBid = currentHighestBid;
        this.currentHighestBidderId = currentHighestBidderId;
        this.status = status;
        this.startTime = startTime;
        this.endTime = endTime;
        this.extensionCount = extensionCount;
        this.winnerId = winnerId;
        this.finalPrice = finalPrice;
        this.paymentDeadline = paymentDeadline;
        this.paymentTimeoutEmitted = paymentTimeoutEmitted;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public static Auction create(String productId, String sellerId, BigDecimal startingPrice,
                                  BigDecimal bidIncrement, Instant startTime, Instant endTime) {
        Instant now = Instant.now();
        return new Auction(UUID.randomUUID().toString(), productId, sellerId, startingPrice, bidIncrement,
                null, null, AuctionStatus.PENDING, startTime, endTime, 0, null, null, null, false, now, now);
    }

    public static Auction reconstitute(String id, String productId, String sellerId, BigDecimal startingPrice,
                                        BigDecimal bidIncrement, BigDecimal currentHighestBid,
                                        String currentHighestBidderId, AuctionStatus status, Instant startTime,
                                        Instant endTime, int extensionCount, String winnerId, BigDecimal finalPrice,
                                        Instant paymentDeadline, boolean paymentTimeoutEmitted, Instant createdAt,
                                        Instant updatedAt) {
        return new Auction(id, productId, sellerId, startingPrice, bidIncrement, currentHighestBid,
                currentHighestBidderId, status, startTime, endTime, extensionCount, winnerId, finalPrice,
                paymentDeadline, paymentTimeoutEmitted, createdAt, updatedAt);
    }

    public Auction withStatus(AuctionStatus newStatus) {
        return new Auction(id, productId, sellerId, startingPrice, bidIncrement, currentHighestBid,
                currentHighestBidderId, newStatus, startTime, endTime, extensionCount, winnerId, finalPrice,
                paymentDeadline, paymentTimeoutEmitted, createdAt, Instant.now());
    }

    public Auction withDetails(BigDecimal newStartingPrice, BigDecimal newBidIncrement,
                                Instant newStartTime, Instant newEndTime) {
        return new Auction(id, productId, sellerId, newStartingPrice, newBidIncrement, currentHighestBid,
                currentHighestBidderId, status, newStartTime, newEndTime, extensionCount, winnerId, finalPrice,
                paymentDeadline, paymentTimeoutEmitted, createdAt, Instant.now());
    }

    public Auction withBid(String bidderId, BigDecimal amount) {
        return new Auction(id, productId, sellerId, startingPrice, bidIncrement, amount, bidderId,
                status, startTime, endTime, extensionCount, winnerId, finalPrice, paymentDeadline,
                paymentTimeoutEmitted, createdAt, Instant.now());
    }

    public Auction withExtendedEndTime(Instant newEndTime) {
        return new Auction(id, productId, sellerId, startingPrice, bidIncrement, currentHighestBid,
                currentHighestBidderId, status, startTime, newEndTime, extensionCount + 1, winnerId, finalPrice,
                paymentDeadline, paymentTimeoutEmitted, createdAt, Instant.now());
    }

    public Auction withSettlement(String newWinnerId, BigDecimal newFinalPrice, Instant newPaymentDeadline) {
        return new Auction(id, productId, sellerId, startingPrice, bidIncrement, currentHighestBid,
                currentHighestBidderId, AuctionStatus.ENDED, startTime, endTime, extensionCount, newWinnerId,
                newFinalPrice, newPaymentDeadline, paymentTimeoutEmitted, createdAt, Instant.now());
    }

    public Auction withPaymentTimeoutEmitted() {
        return new Auction(id, productId, sellerId, startingPrice, bidIncrement, currentHighestBid,
                currentHighestBidderId, status, startTime, endTime, extensionCount, winnerId, finalPrice,
                paymentDeadline, true, createdAt, Instant.now());
    }

    public String getId() { return id; }
    public String getProductId() { return productId; }
    public String getSellerId() { return sellerId; }
    public BigDecimal getStartingPrice() { return startingPrice; }
    public BigDecimal getBidIncrement() { return bidIncrement; }
    public BigDecimal getCurrentHighestBid() { return currentHighestBid; }
    public String getCurrentHighestBidderId() { return currentHighestBidderId; }
    public AuctionStatus getStatus() { return status; }
    public Instant getStartTime() { return startTime; }
    public Instant getEndTime() { return endTime; }
    public int getExtensionCount() { return extensionCount; }
    public String getWinnerId() { return winnerId; }
    public BigDecimal getFinalPrice() { return finalPrice; }
    public Instant getPaymentDeadline() { return paymentDeadline; }
    public boolean isPaymentTimeoutEmitted() { return paymentTimeoutEmitted; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=AuctionTest test`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/nexus/auction/domain/model/AuctionStatus.java \
  src/main/java/com/nexus/auction/domain/model/Auction.java \
  src/test/java/com/nexus/auction/domain/model/AuctionTest.java
git commit -m "feat: add Auction domain model"
```

---

### Task 4: Domain model — `Bid`

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/domain/model/Bid.java`
- Test: `auction-service/src/test/java/com/nexus/auction/domain/model/BidTest.java`

**Interfaces:**
- Consumes: nothing beyond the JDK.
- Produces: `Bid` (immutable value object) — consumed by Task 8 (persistence), Task 13 (`PlaceBidUseCase`), Task 14 (`GetBidHistoryUseCase`).

- [ ] **Step 1: Write the failing test**

```java
package com.nexus.auction.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class BidTest {

    @Test
    void create_generatesIdAndPlacedAtTimestamp() {
        Bid bid = Bid.create("auction-1", "bidder-1", new BigDecimal("120.00"));

        assertThat(bid.getId()).isNotBlank();
        assertThat(bid.getAuctionId()).isEqualTo("auction-1");
        assertThat(bid.getBidderId()).isEqualTo("bidder-1");
        assertThat(bid.getAmount()).isEqualByComparingTo("120.00");
        assertThat(bid.getPlacedAt()).isNotNull();
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/FPT/auction-service && mvn -Dtest=BidTest test`
Expected: FAIL with "cannot find symbol: class Bid".

- [ ] **Step 3: Write `Bid`**

```java
package com.nexus.auction.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class Bid {

    private final String id;
    private final String auctionId;
    private final String bidderId;
    private final BigDecimal amount;
    private final Instant placedAt;

    private Bid(String id, String auctionId, String bidderId, BigDecimal amount, Instant placedAt) {
        this.id = id;
        this.auctionId = auctionId;
        this.bidderId = bidderId;
        this.amount = amount;
        this.placedAt = placedAt;
    }

    public static Bid create(String auctionId, String bidderId, BigDecimal amount) {
        return new Bid(UUID.randomUUID().toString(), auctionId, bidderId, amount, Instant.now());
    }

    public static Bid reconstitute(String id, String auctionId, String bidderId, BigDecimal amount, Instant placedAt) {
        return new Bid(id, auctionId, bidderId, amount, placedAt);
    }

    public String getId() { return id; }
    public String getAuctionId() { return auctionId; }
    public String getBidderId() { return bidderId; }
    public BigDecimal getAmount() { return amount; }
    public Instant getPlacedAt() { return placedAt; }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=BidTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/nexus/auction/domain/model/Bid.java \
  src/test/java/com/nexus/auction/domain/model/BidTest.java
git commit -m "feat: add Bid domain model"
```

---

### Task 5: Domain policy — `BidValidationPolicy`

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/domain/service/BidValidationPolicy.java`
- Test: `auction-service/src/test/java/com/nexus/auction/domain/service/BidValidationPolicyTest.java`

**Interfaces:**
- Consumes: `Auction` (Task 3), `AuctionStatus` (Task 3), `com.nexus.common.core.exception.ConflictException`, `com.nexus.common.core.exception.ValidationException`, `com.nexus.common.core.FieldError` (existing `common-core` classes).
- Produces: `static void BidValidationPolicy.validate(Auction auction, BigDecimal amount)` — throws on invalid input, returns normally otherwise. Consumed by Task 13's `PlaceBidUseCase`.

- [ ] **Step 1: Write the failing test**

```java
package com.nexus.auction.domain.service;

import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.core.exception.ConflictException;
import com.nexus.common.core.exception.ValidationException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BidValidationPolicyTest {

    private Auction activeAuctionNoBidsYet() {
        return Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                        Instant.now().minus(1, ChronoUnit.HOURS), Instant.now().plus(1, ChronoUnit.HOURS))
                .withStatus(AuctionStatus.ACTIVE);
    }

    @Test
    void validate_acceptsBidEqualToStartingPriceWhenNoBidsYet() {
        Auction auction = activeAuctionNoBidsYet();

        assertThatCode(() -> BidValidationPolicy.validate(auction, new BigDecimal("100.00")))
                .doesNotThrowAnyException();
    }

    @Test
    void validate_rejectsBidBelowStartingPriceWhenNoBidsYet() {
        Auction auction = activeAuctionNoBidsYet();

        assertThatThrownBy(() -> BidValidationPolicy.validate(auction, new BigDecimal("99.99")))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void validate_acceptsBidExactlyAtHighestPlusIncrement() {
        Auction auction = activeAuctionNoBidsYet().withBid("bidder-1", new BigDecimal("100.00"));

        // 100.00 (current highest) + 10.00 (increment) = 110.00 — must be accepted, not rejected
        // for being "not strictly greater". This is the Review Focus off-by-one case.
        assertThatCode(() -> BidValidationPolicy.validate(auction, new BigDecimal("110.00")))
                .doesNotThrowAnyException();
    }

    @Test
    void validate_rejectsBidBelowHighestPlusIncrement() {
        Auction auction = activeAuctionNoBidsYet().withBid("bidder-1", new BigDecimal("100.00"));

        assertThatThrownBy(() -> BidValidationPolicy.validate(auction, new BigDecimal("109.99")))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void validate_rejectsBidOnPendingAuction() {
        Auction pending = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                Instant.now().plus(1, ChronoUnit.HOURS), Instant.now().plus(2, ChronoUnit.HOURS));

        assertThatThrownBy(() -> BidValidationPolicy.validate(pending, new BigDecimal("100.00")))
                .isInstanceOf(ConflictException.class);
    }

    @Test
    void validate_rejectsBidOnEndedAuction() {
        Auction ended = activeAuctionNoBidsYet().withStatus(AuctionStatus.ENDED);

        assertThatThrownBy(() -> BidValidationPolicy.validate(ended, new BigDecimal("100.00")))
                .isInstanceOf(ConflictException.class);
    }

    @Test
    void validate_rejectsBidOnCancelledAuction() {
        Auction cancelled = activeAuctionNoBidsYet().withStatus(AuctionStatus.CANCELLED);

        assertThatThrownBy(() -> BidValidationPolicy.validate(cancelled, new BigDecimal("100.00")))
                .isInstanceOf(ConflictException.class);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/FPT/auction-service && mvn -Dtest=BidValidationPolicyTest test`
Expected: FAIL with "cannot find symbol: class BidValidationPolicy".

- [ ] **Step 3: Write `BidValidationPolicy`**

```java
package com.nexus.auction.domain.service;

import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.core.FieldError;
import com.nexus.common.core.exception.ConflictException;
import com.nexus.common.core.exception.ValidationException;

import java.math.BigDecimal;
import java.util.List;

public final class BidValidationPolicy {

    private BidValidationPolicy() {
    }

    public static void validate(Auction auction, BigDecimal amount) {
        if (auction.getStatus() != AuctionStatus.ACTIVE) {
            throw new ConflictException("AUCTION_NOT_ACTIVE",
                    "Cannot bid on an auction that is not active: " + auction.getId());
        }

        BigDecimal minimum = auction.getCurrentHighestBid() == null
                ? auction.getStartingPrice()
                : auction.getCurrentHighestBid().add(auction.getBidIncrement());

        if (amount.compareTo(minimum) < 0) {
            throw new ValidationException(List.of(new FieldError("amount",
                    "Bid must be at least " + minimum)));
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=BidValidationPolicyTest test`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/nexus/auction/domain/service/BidValidationPolicy.java \
  src/test/java/com/nexus/auction/domain/service/BidValidationPolicyTest.java
git commit -m "feat: add BidValidationPolicy"
```

---

### Task 6: Domain policy — `AntiSnipingPolicy`

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/domain/service/AntiSnipingPolicy.java`
- Test: `auction-service/src/test/java/com/nexus/auction/domain/service/AntiSnipingPolicyTest.java`

**Interfaces:**
- Consumes: `Auction` (Task 3).
- Produces: `static Optional<Instant> AntiSnipingPolicy.tryExtend(Auction auction, Instant now)` — returns the new `end_time` if an extension should happen, else `Optional.empty()`. Consumed by Task 13's `PlaceBidUseCase`.

- [ ] **Step 1: Write the failing test**

```java
package com.nexus.auction.domain.service;

import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class AntiSnipingPolicyTest {

    @Test
    void tryExtend_extendsWhenBidArrivesWithinFiveMinutesOfEnd() {
        Instant now = Instant.now();
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                        now.minus(1, ChronoUnit.HOURS), now.plus(3, ChronoUnit.MINUTES))
                .withStatus(AuctionStatus.ACTIVE);

        Optional<Instant> extended = AntiSnipingPolicy.tryExtend(auction, now);

        assertThat(extended).isPresent();
        assertThat(extended.get()).isEqualTo(auction.getEndTime().plus(5, ChronoUnit.MINUTES));
    }

    @Test
    void tryExtend_doesNotExtendWhenBidArrivesOutsideTheWindow() {
        Instant now = Instant.now();
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                        now.minus(1, ChronoUnit.HOURS), now.plus(30, ChronoUnit.MINUTES))
                .withStatus(AuctionStatus.ACTIVE);

        Optional<Instant> extended = AntiSnipingPolicy.tryExtend(auction, now);

        assertThat(extended).isEmpty();
    }

    @Test
    void tryExtend_stopsExtendingOnceMaxExtensionsReached() {
        Instant now = Instant.now();
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                        now.minus(1, ChronoUnit.HOURS), now.plus(2, ChronoUnit.MINUTES))
                .withStatus(AuctionStatus.ACTIVE);
        // Simulate 12 prior extensions (MAX_AUCTION_EXTENSIONS) via repeated withExtendedEndTime.
        for (int i = 0; i < 12; i++) {
            auction = auction.withExtendedEndTime(auction.getEndTime().plus(5, ChronoUnit.MINUTES));
        }
        assertThat(auction.getExtensionCount()).isEqualTo(12);

        Optional<Instant> extended = AntiSnipingPolicy.tryExtend(auction, now);

        assertThat(extended).isEmpty();
    }

    @Test
    void tryExtend_allowsTheTwelfthExtensionExactlyAtTheCap() {
        Instant now = Instant.now();
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                        now.minus(1, ChronoUnit.HOURS), now.plus(2, ChronoUnit.MINUTES))
                .withStatus(AuctionStatus.ACTIVE);
        for (int i = 0; i < 11; i++) {
            auction = auction.withExtendedEndTime(auction.getEndTime().plus(5, ChronoUnit.MINUTES));
        }
        assertThat(auction.getExtensionCount()).isEqualTo(11);

        Optional<Instant> extended = AntiSnipingPolicy.tryExtend(auction, now);

        assertThat(extended).isPresent();
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/FPT/auction-service && mvn -Dtest=AntiSnipingPolicyTest test`
Expected: FAIL with "cannot find symbol: class AntiSnipingPolicy".

- [ ] **Step 3: Write `AntiSnipingPolicy`**

```java
package com.nexus.auction.domain.service;

import com.nexus.auction.domain.model.Auction;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

public final class AntiSnipingPolicy {

    public static final long EXTENSION_MINUTES = 5;
    public static final int MAX_EXTENSIONS = 12;

    private AntiSnipingPolicy() {
    }

    public static Optional<Instant> tryExtend(Auction auction, Instant now) {
        if (auction.getExtensionCount() >= MAX_EXTENSIONS) {
            return Optional.empty();
        }
        Instant window = auction.getEndTime().minus(EXTENSION_MINUTES, ChronoUnit.MINUTES);
        if (now.isBefore(window)) {
            return Optional.empty();
        }
        return Optional.of(auction.getEndTime().plus(EXTENSION_MINUTES, ChronoUnit.MINUTES));
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=AntiSnipingPolicyTest test`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/nexus/auction/domain/service/AntiSnipingPolicy.java \
  src/test/java/com/nexus/auction/domain/service/AntiSnipingPolicyTest.java
git commit -m "feat: add AntiSnipingPolicy"
```

---

### Task 7: Persistence — `auctions` table, `AuctionRepositoryPort`/`AuctionRepositoryAdapter`

**Files:**
- Create: `auction-service/src/main/resources/db/migration/V1__create_auctions_table.sql`, `auction-service/src/main/java/com/nexus/auction/infrastructure/persistence/UuidIds.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/persistence/entity/AuctionJpaEntity.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/persistence/AuctionJpaRepository.java`, `auction-service/src/main/java/com/nexus/auction/application/port/out/AuctionRepositoryPort.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/persistence/AuctionRepositoryAdapter.java`
- Test: `auction-service/src/test/java/com/nexus/auction/infrastructure/persistence/AuctionRepositoryAdapterTest.java`

**Interfaces:**
- Consumes: `Auction`/`AuctionStatus` (Task 3).
- Produces: `AuctionRepositoryPort` — `save`, `findById`, `findByIdForUpdate` (pessimistic lock, used by Task 13), `existsActiveOrPendingForProduct(String productId)`, `countActiveOrPendingBySeller(String sellerId)`, `search(...)`, `findPendingReadyToStart(Instant now)`, `findActiveReadyToEnd(Instant now)`, `findEndedAwaitingPaymentTimeout(Instant now)` — the last three consumed by Task 16/17's scheduled jobs.

- [ ] **Step 1: Write the migration**

```sql
-- V1__create_auctions_table.sql
CREATE TABLE auctions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id UUID NOT NULL,
    seller_id VARCHAR(255) NOT NULL,
    starting_price NUMERIC(12,2) NOT NULL,
    bid_increment NUMERIC(12,2) NOT NULL,
    current_highest_bid NUMERIC(12,2),
    current_highest_bidder_id VARCHAR(255),
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    extension_count INT NOT NULL DEFAULT 0,
    winner_id VARCHAR(255),
    final_price NUMERIC(12,2),
    payment_deadline TIMESTAMPTZ,
    payment_timeout_emitted BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_auctions_product_id ON auctions (product_id);
CREATE INDEX idx_auctions_seller_id ON auctions (seller_id);
CREATE INDEX idx_auctions_status ON auctions (status);
```

- [ ] **Step 2: Write `UuidIds` (copied verbatim from `catalog-service`, same package-private helper)**

```java
package com.nexus.auction.infrastructure.persistence;

import java.util.Optional;
import java.util.UUID;

final class UuidIds {

    private UuidIds() {
    }

    static Optional<UUID> tryParse(String id) {
        if (id == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(UUID.fromString(id));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
```

- [ ] **Step 3: Write `AuctionJpaEntity`**

```java
package com.nexus.auction.infrastructure.persistence.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "auctions")
public class AuctionJpaEntity {

    @Id
    private UUID id;

    @Column(name = "product_id", nullable = false)
    private UUID productId;

    @Column(name = "seller_id", nullable = false)
    private String sellerId;

    @Column(name = "starting_price", nullable = false)
    private BigDecimal startingPrice;

    @Column(name = "bid_increment", nullable = false)
    private BigDecimal bidIncrement;

    @Column(name = "current_highest_bid")
    private BigDecimal currentHighestBid;

    @Column(name = "current_highest_bidder_id")
    private String currentHighestBidderId;

    @Column(nullable = false)
    private String status;

    @Column(name = "start_time", nullable = false)
    private Instant startTime;

    @Column(name = "end_time", nullable = false)
    private Instant endTime;

    @Column(name = "extension_count", nullable = false)
    private int extensionCount;

    @Column(name = "winner_id")
    private String winnerId;

    @Column(name = "final_price")
    private BigDecimal finalPrice;

    @Column(name = "payment_deadline")
    private Instant paymentDeadline;

    @Column(name = "payment_timeout_emitted", nullable = false)
    private boolean paymentTimeoutEmitted;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected AuctionJpaEntity() {
    }

    public AuctionJpaEntity(UUID id, UUID productId, String sellerId, BigDecimal startingPrice,
                             BigDecimal bidIncrement, BigDecimal currentHighestBid, String currentHighestBidderId,
                             String status, Instant startTime, Instant endTime, int extensionCount, String winnerId,
                             BigDecimal finalPrice, Instant paymentDeadline, boolean paymentTimeoutEmitted,
                             Instant createdAt, Instant updatedAt) {
        this.id = id;
        this.productId = productId;
        this.sellerId = sellerId;
        this.startingPrice = startingPrice;
        this.bidIncrement = bidIncrement;
        this.currentHighestBid = currentHighestBid;
        this.currentHighestBidderId = currentHighestBidderId;
        this.status = status;
        this.startTime = startTime;
        this.endTime = endTime;
        this.extensionCount = extensionCount;
        this.winnerId = winnerId;
        this.finalPrice = finalPrice;
        this.paymentDeadline = paymentDeadline;
        this.paymentTimeoutEmitted = paymentTimeoutEmitted;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public UUID getId() { return id; }
    public UUID getProductId() { return productId; }
    public String getSellerId() { return sellerId; }
    public BigDecimal getStartingPrice() { return startingPrice; }
    public BigDecimal getBidIncrement() { return bidIncrement; }
    public BigDecimal getCurrentHighestBid() { return currentHighestBid; }
    public String getCurrentHighestBidderId() { return currentHighestBidderId; }
    public String getStatus() { return status; }
    public Instant getStartTime() { return startTime; }
    public Instant getEndTime() { return endTime; }
    public int getExtensionCount() { return extensionCount; }
    public String getWinnerId() { return winnerId; }
    public BigDecimal getFinalPrice() { return finalPrice; }
    public Instant getPaymentDeadline() { return paymentDeadline; }
    public boolean isPaymentTimeoutEmitted() { return paymentTimeoutEmitted; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
```

- [ ] **Step 4: Write `AuctionJpaRepository`**

```java
package com.nexus.auction.infrastructure.persistence;

import com.nexus.auction.infrastructure.persistence.entity.AuctionJpaEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface AuctionJpaRepository extends JpaRepository<AuctionJpaEntity, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT a FROM AuctionJpaEntity a WHERE a.id = :id")
    Optional<AuctionJpaEntity> findByIdForUpdate(@Param("id") UUID id);

    boolean existsByProductIdAndStatusIn(UUID productId, List<String> statuses);

    long countBySellerIdAndStatusIn(String sellerId, List<String> statuses);

    @Query(value = """
            SELECT * FROM auctions a
            WHERE (:status IS NULL OR a.status = :status)
              AND (:sellerId IS NULL OR a.seller_id = :sellerId)
              AND (:productId IS NULL OR a.product_id = CAST(:productId AS uuid))
            ORDER BY a.created_at DESC
            LIMIT :size OFFSET :offset
            """, nativeQuery = true)
    List<AuctionJpaEntity> search(
            @Param("status") String status,
            @Param("sellerId") String sellerId,
            @Param("productId") String productId,
            @Param("size") int size,
            @Param("offset") int offset);

    List<AuctionJpaEntity> findByStatusAndStartTimeLessThanEqual(String status, Instant now);

    List<AuctionJpaEntity> findByStatusAndEndTimeLessThanEqual(String status, Instant now);

    List<AuctionJpaEntity> findByStatusAndWinnerIdIsNotNullAndPaymentTimeoutEmittedFalseAndPaymentDeadlineLessThanEqual(
            String status, Instant now);
}
```

- [ ] **Step 5: Write `AuctionRepositoryPort`**

```java
package com.nexus.auction.application.port.out;

import com.nexus.auction.domain.model.Auction;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface AuctionRepositoryPort {
    Auction save(Auction auction);
    Optional<Auction> findById(String id);
    Optional<Auction> findByIdForUpdate(String id);
    boolean existsActiveOrPendingForProduct(String productId);
    long countActiveOrPendingBySeller(String sellerId);
    List<Auction> search(String status, String sellerId, String productId, int page, int size);
    List<Auction> findPendingReadyToStart(Instant now);
    List<Auction> findActiveReadyToEnd(Instant now);
    List<Auction> findEndedAwaitingPaymentTimeout(Instant now);
}
```

- [ ] **Step 6: Write `AuctionRepositoryAdapter`**

```java
package com.nexus.auction.infrastructure.persistence;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.auction.infrastructure.persistence.entity.AuctionJpaEntity;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
public class AuctionRepositoryAdapter implements AuctionRepositoryPort {

    private static final List<String> ACTIVE_OR_PENDING =
            List.of(AuctionStatus.PENDING.name(), AuctionStatus.ACTIVE.name());

    private final AuctionJpaRepository jpaRepository;

    public AuctionRepositoryAdapter(AuctionJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public Auction save(Auction auction) {
        AuctionJpaEntity entity = toEntity(auction);
        jpaRepository.save(entity);
        return auction;
    }

    @Override
    public Optional<Auction> findById(String id) {
        return UuidIds.tryParse(id).flatMap(jpaRepository::findById).map(this::toDomain);
    }

    @Override
    public Optional<Auction> findByIdForUpdate(String id) {
        return UuidIds.tryParse(id).flatMap(jpaRepository::findByIdForUpdate).map(this::toDomain);
    }

    @Override
    public boolean existsActiveOrPendingForProduct(String productId) {
        return UuidIds.tryParse(productId)
                .map(uuid -> jpaRepository.existsByProductIdAndStatusIn(uuid, ACTIVE_OR_PENDING))
                .orElse(false);
    }

    @Override
    public long countActiveOrPendingBySeller(String sellerId) {
        return jpaRepository.countBySellerIdAndStatusIn(sellerId, ACTIVE_OR_PENDING);
    }

    @Override
    public List<Auction> search(String status, String sellerId, String productId, int page, int size) {
        int offset = page * size;
        return jpaRepository.search(status, sellerId, productId, size, offset).stream()
                .map(this::toDomain).toList();
    }

    @Override
    public List<Auction> findPendingReadyToStart(Instant now) {
        return jpaRepository.findByStatusAndStartTimeLessThanEqual(AuctionStatus.PENDING.name(), now).stream()
                .map(this::toDomain).toList();
    }

    @Override
    public List<Auction> findActiveReadyToEnd(Instant now) {
        return jpaRepository.findByStatusAndEndTimeLessThanEqual(AuctionStatus.ACTIVE.name(), now).stream()
                .map(this::toDomain).toList();
    }

    @Override
    public List<Auction> findEndedAwaitingPaymentTimeout(Instant now) {
        return jpaRepository.findByStatusAndWinnerIdIsNotNullAndPaymentTimeoutEmittedFalseAndPaymentDeadlineLessThanEqual(
                AuctionStatus.ENDED.name(), now).stream().map(this::toDomain).toList();
    }

    private AuctionJpaEntity toEntity(Auction a) {
        return new AuctionJpaEntity(UUID.fromString(a.getId()), UUID.fromString(a.getProductId()), a.getSellerId(),
                a.getStartingPrice(), a.getBidIncrement(), a.getCurrentHighestBid(), a.getCurrentHighestBidderId(),
                a.getStatus().name(), a.getStartTime(), a.getEndTime(), a.getExtensionCount(), a.getWinnerId(),
                a.getFinalPrice(), a.getPaymentDeadline(), a.isPaymentTimeoutEmitted(), a.getCreatedAt(), a.getUpdatedAt());
    }

    private Auction toDomain(AuctionJpaEntity e) {
        return Auction.reconstitute(e.getId().toString(), e.getProductId().toString(), e.getSellerId(),
                e.getStartingPrice(), e.getBidIncrement(), e.getCurrentHighestBid(), e.getCurrentHighestBidderId(),
                AuctionStatus.valueOf(e.getStatus()), e.getStartTime(), e.getEndTime(), e.getExtensionCount(),
                e.getWinnerId(), e.getFinalPrice(), e.getPaymentDeadline(), e.isPaymentTimeoutEmitted(),
                e.getCreatedAt(), e.getUpdatedAt());
    }
}
```

- [ ] **Step 7: Write the adapter test**

```java
package com.nexus.auction.infrastructure.persistence;

import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(AuctionRepositoryAdapter.class)
class AuctionRepositoryAdapterTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("auction_db").withUsername("nexus").withPassword("nexus");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired private AuctionRepositoryAdapter adapter;

    private Auction newAuction() {
        return Auction.create("11111111-1111-1111-1111-111111111111", "seller-1",
                new BigDecimal("100.00"), new BigDecimal("10.00"),
                Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS));
    }

    @Test
    void saveThenFindById_roundTripsTheAuction() {
        Auction auction = newAuction();

        adapter.save(auction);
        Optional<Auction> found = adapter.findById(auction.getId());

        assertThat(found).isPresent();
        assertThat(found.get().getStatus()).isEqualTo(AuctionStatus.PENDING);
        assertThat(found.get().getStartingPrice()).isEqualByComparingTo("100.00");
    }

    @Test
    void findByIdForUpdate_returnsTheSameRowAsFindById() {
        Auction auction = newAuction();
        adapter.save(auction);

        Optional<Auction> found = adapter.findByIdForUpdate(auction.getId());

        assertThat(found).isPresent();
        assertThat(found.get().getId()).isEqualTo(auction.getId());
    }

    @Test
    void existsActiveOrPendingForProduct_trueWhilePendingOrActive() {
        Auction auction = newAuction();
        adapter.save(auction);

        assertThat(adapter.existsActiveOrPendingForProduct(auction.getProductId())).isTrue();
    }

    @Test
    void existsActiveOrPendingForProduct_falseOnceEnded() {
        Auction auction = newAuction().withStatus(AuctionStatus.ENDED);
        adapter.save(auction);

        assertThat(adapter.existsActiveOrPendingForProduct(auction.getProductId())).isFalse();
    }

    @Test
    void countActiveOrPendingBySeller_countsOnlyThatSellersPendingAndActive() {
        adapter.save(newAuction());
        adapter.save(newAuction().withStatus(AuctionStatus.CANCELLED));

        assertThat(adapter.countActiveOrPendingBySeller("seller-1")).isEqualTo(1);
    }

    @Test
    void findById_returnsEmptyForMalformedId() {
        assertThat(adapter.findById("not-a-uuid")).isEmpty();
    }
}
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cd /c/FPT/auction-service && mvn -Dtest=AuctionRepositoryAdapterTest test`
Expected: PASS (6 tests). Requires Docker running (Testcontainers).

- [ ] **Step 9: Commit**

```bash
git add src/main/resources/db/migration/V1__create_auctions_table.sql \
  src/main/java/com/nexus/auction/infrastructure/persistence/UuidIds.java \
  src/main/java/com/nexus/auction/infrastructure/persistence/entity/AuctionJpaEntity.java \
  src/main/java/com/nexus/auction/infrastructure/persistence/AuctionJpaRepository.java \
  src/main/java/com/nexus/auction/application/port/out/AuctionRepositoryPort.java \
  src/main/java/com/nexus/auction/infrastructure/persistence/AuctionRepositoryAdapter.java \
  src/test/java/com/nexus/auction/infrastructure/persistence/AuctionRepositoryAdapterTest.java
git commit -m "feat: add auctions table and AuctionRepositoryAdapter"
```

---

### Task 8: Persistence — `bids` table, `BidRepositoryPort`/`BidRepositoryAdapter`

**Files:**
- Create: `auction-service/src/main/resources/db/migration/V2__create_bids_table.sql`, `auction-service/src/main/java/com/nexus/auction/infrastructure/persistence/entity/BidJpaEntity.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/persistence/BidJpaRepository.java`, `auction-service/src/main/java/com/nexus/auction/application/port/out/BidRepositoryPort.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/persistence/BidRepositoryAdapter.java`
- Test: `auction-service/src/test/java/com/nexus/auction/infrastructure/persistence/BidRepositoryAdapterTest.java`

**Interfaces:**
- Consumes: `Bid` (Task 4), `UuidIds` (Task 7).
- Produces: `BidRepositoryPort` — `save`, `findByAuctionId(String auctionId, int page, int size)` (newest first) — consumed by Task 13 and Task 14.

- [ ] **Step 1: Write the migration**

```sql
-- V2__create_bids_table.sql
CREATE TABLE bids (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    auction_id UUID NOT NULL REFERENCES auctions(id),
    bidder_id VARCHAR(255) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    placed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_bids_auction_id ON bids (auction_id);
```

- [ ] **Step 2: Write `BidJpaEntity`**

```java
package com.nexus.auction.infrastructure.persistence.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "bids")
public class BidJpaEntity {

    @Id
    private UUID id;

    @Column(name = "auction_id", nullable = false)
    private UUID auctionId;

    @Column(name = "bidder_id", nullable = false)
    private String bidderId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(name = "placed_at", nullable = false)
    private Instant placedAt;

    protected BidJpaEntity() {
    }

    public BidJpaEntity(UUID id, UUID auctionId, String bidderId, BigDecimal amount, Instant placedAt) {
        this.id = id;
        this.auctionId = auctionId;
        this.bidderId = bidderId;
        this.amount = amount;
        this.placedAt = placedAt;
    }

    public UUID getId() { return id; }
    public UUID getAuctionId() { return auctionId; }
    public String getBidderId() { return bidderId; }
    public BigDecimal getAmount() { return amount; }
    public Instant getPlacedAt() { return placedAt; }
}
```

- [ ] **Step 3: Write `BidJpaRepository`**

```java
package com.nexus.auction.infrastructure.persistence;

import com.nexus.auction.infrastructure.persistence.entity.BidJpaEntity;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface BidJpaRepository extends JpaRepository<BidJpaEntity, UUID> {
    List<BidJpaEntity> findByAuctionIdOrderByPlacedAtDesc(UUID auctionId, PageRequest pageRequest);
}
```

- [ ] **Step 4: Write `BidRepositoryPort`**

```java
package com.nexus.auction.application.port.out;

import com.nexus.auction.domain.model.Bid;

import java.util.List;

public interface BidRepositoryPort {
    Bid save(Bid bid);
    List<Bid> findByAuctionId(String auctionId, int page, int size);
}
```

- [ ] **Step 5: Write `BidRepositoryAdapter`**

```java
package com.nexus.auction.infrastructure.persistence;

import com.nexus.auction.application.port.out.BidRepositoryPort;
import com.nexus.auction.domain.model.Bid;
import com.nexus.auction.infrastructure.persistence.entity.BidJpaEntity;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class BidRepositoryAdapter implements BidRepositoryPort {

    private final BidJpaRepository jpaRepository;

    public BidRepositoryAdapter(BidJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public Bid save(Bid bid) {
        BidJpaEntity entity = new BidJpaEntity(UUID.fromString(bid.getId()), UUID.fromString(bid.getAuctionId()),
                bid.getBidderId(), bid.getAmount(), bid.getPlacedAt());
        jpaRepository.save(entity);
        return bid;
    }

    @Override
    public List<Bid> findByAuctionId(String auctionId, int page, int size) {
        return UuidIds.tryParse(auctionId)
                .map(uuid -> jpaRepository.findByAuctionIdOrderByPlacedAtDesc(uuid, PageRequest.of(page, size))
                        .stream().map(this::toDomain).toList())
                .orElse(List.of());
    }

    private Bid toDomain(BidJpaEntity e) {
        return Bid.reconstitute(e.getId().toString(), e.getAuctionId().toString(), e.getBidderId(),
                e.getAmount(), e.getPlacedAt());
    }
}
```

- [ ] **Step 6: Write the adapter test**

```java
package com.nexus.auction.infrastructure.persistence;

import com.nexus.auction.domain.model.Bid;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(BidRepositoryAdapter.class)
class BidRepositoryAdapterTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("auction_db").withUsername("nexus").withPassword("nexus");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired private BidRepositoryAdapter adapter;
    @Autowired private JdbcTemplate jdbcTemplate;

    private String seedAuction() {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO auctions (id, product_id, seller_id, starting_price, bid_increment, status,
                                       start_time, end_time)
                VALUES (?, ?, 'seller-1', 100.00, 10.00, 'ACTIVE', now(), now() + interval '1 hour')
                """, id, UUID.randomUUID());
        return id.toString();
    }

    @Test
    void saveThenFindByAuctionId_returnsNewestFirst() {
        String auctionId = seedAuction();
        Bid first = Bid.create(auctionId, "bidder-1", new BigDecimal("100.00"));
        adapter.save(first);
        Bid second = Bid.create(auctionId, "bidder-2", new BigDecimal("110.00"));
        adapter.save(second);

        List<Bid> bids = adapter.findByAuctionId(auctionId, 0, 10);

        assertThat(bids).hasSize(2);
        assertThat(bids.get(0).getBidderId()).isEqualTo("bidder-2");
    }

    @Test
    void findByAuctionId_returnsEmptyForMalformedId() {
        assertThat(adapter.findByAuctionId("not-a-uuid", 0, 10)).isEmpty();
    }
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd /c/FPT/auction-service && mvn -Dtest=BidRepositoryAdapterTest test`
Expected: PASS (2 tests).

- [ ] **Step 8: Commit**

```bash
git add src/main/resources/db/migration/V2__create_bids_table.sql \
  src/main/java/com/nexus/auction/infrastructure/persistence/entity/BidJpaEntity.java \
  src/main/java/com/nexus/auction/infrastructure/persistence/BidJpaRepository.java \
  src/main/java/com/nexus/auction/application/port/out/BidRepositoryPort.java \
  src/main/java/com/nexus/auction/infrastructure/persistence/BidRepositoryAdapter.java \
  src/test/java/com/nexus/auction/infrastructure/persistence/BidRepositoryAdapterTest.java
git commit -m "feat: add bids table and BidRepositoryAdapter"
```

---

### Task 9: Outbox — `outbox` table, `EventPublisherPort`, `OutboxEventPublisherAdapter`, `OutboxRelayJob`

**Files:**
- Create: `auction-service/src/main/resources/db/migration/V3__create_outbox_table.sql`, `auction-service/src/main/java/com/nexus/auction/infrastructure/persistence/entity/OutboxJpaEntity.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/persistence/OutboxJpaRepository.java`, `auction-service/src/main/java/com/nexus/auction/application/port/out/EventPublisherPort.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/messaging/OutboxEventPublisherAdapter.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/messaging/OutboxRelayJob.java`
- Test: `auction-service/src/test/java/com/nexus/auction/infrastructure/messaging/OutboxRelayJobTest.java`

**Interfaces:**
- Consumes: `com.nexus.common.events.DomainEvent` (Task 2).
- Produces: `EventPublisherPort.publish(DomainEvent)` — consumed by every use case task from Task 10 onward. Publishes to Kafka topic `auction-events`.

- [ ] **Step 1: Write the migration**

```sql
-- V3__create_outbox_table.sql
CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
```

- [ ] **Step 2: Write `OutboxJpaEntity`, `OutboxJpaRepository` (identical shape to `catalog-service`'s)**

```java
package com.nexus.auction.infrastructure.persistence.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox")
public class OutboxJpaEntity {

    @Id
    private UUID id;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxJpaEntity() {
    }

    public OutboxJpaEntity(UUID id, String aggregateId, String eventType, String payload, Instant createdAt) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    public UUID getId() { return id; }
    public String getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getPublishedAt() { return publishedAt; }
    public void markPublished(Instant when) { this.publishedAt = when; }
}
```

```java
package com.nexus.auction.infrastructure.persistence;

import com.nexus.auction.infrastructure.persistence.entity.OutboxJpaEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxJpaRepository extends JpaRepository<OutboxJpaEntity, UUID> {
    List<OutboxJpaEntity> findTop50ByPublishedAtIsNullOrderByCreatedAtAsc();
}
```

- [ ] **Step 3: Write `EventPublisherPort` and `OutboxEventPublisherAdapter`**

```java
package com.nexus.auction.application.port.out;

import com.nexus.common.events.DomainEvent;

public interface EventPublisherPort {
    void publish(DomainEvent event);
}
```

```java
package com.nexus.auction.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.infrastructure.persistence.OutboxJpaRepository;
import com.nexus.auction.infrastructure.persistence.entity.OutboxJpaEntity;
import com.nexus.common.events.DomainEvent;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class OutboxEventPublisherAdapter implements EventPublisherPort {

    private final OutboxJpaRepository outboxJpaRepository;
    private final ObjectMapper objectMapper;

    public OutboxEventPublisherAdapter(OutboxJpaRepository outboxJpaRepository, ObjectMapper objectMapper) {
        this.outboxJpaRepository = outboxJpaRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(DomainEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            OutboxJpaEntity entity = new OutboxJpaEntity(
                    UUID.randomUUID(), event.getAggregateId(), event.getEventType(), payload, event.getOccurredAt());
            outboxJpaRepository.save(entity);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize event for outbox: " + event.getEventType(), e);
        }
    }
}
```

- [ ] **Step 4: Write `OutboxRelayJob`**

```java
package com.nexus.auction.infrastructure.messaging;

import com.nexus.auction.infrastructure.persistence.OutboxJpaRepository;
import com.nexus.auction.infrastructure.persistence.entity.OutboxJpaEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxRelayJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayJob.class);
    private static final String TOPIC = "auction-events";

    private final OutboxJpaRepository outboxJpaRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelayJob(OutboxJpaRepository outboxJpaRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxJpaRepository = outboxJpaRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 5000)
    public synchronized void relayPendingEvents() {
        List<OutboxJpaEntity> pending = outboxJpaRepository.findTop50ByPublishedAtIsNullOrderByCreatedAtAsc();
        for (OutboxJpaEntity row : pending) {
            try {
                kafkaTemplate.send(TOPIC, row.getAggregateId(), row.getPayload()).get(5, TimeUnit.SECONDS);
                row.markPublished(Instant.now());
                outboxJpaRepository.save(row);
            } catch (Exception e) {
                log.error("Failed to publish outbox row {} (event {}) — will retry on next poll",
                        row.getId(), row.getEventType(), e);
            }
        }
    }
}
```

- [ ] **Step 5: Write the relay test (Testcontainers Postgres + Kafka, mirrors `catalog-service`'s `OutboxRelayJobTest`)**

```java
package com.nexus.auction.infrastructure.messaging;

import com.nexus.auction.infrastructure.persistence.OutboxJpaRepository;
import com.nexus.auction.infrastructure.persistence.entity.OutboxJpaEntity;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class OutboxRelayJobTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("auction_db").withUsername("nexus").withPassword("nexus");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.1"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("eureka.client.enabled", () -> "false");
    }

    @Autowired private OutboxJpaRepository outboxJpaRepository;
    @Autowired private OutboxRelayJob outboxRelayJob;

    private KafkaConsumer<String, String> consumer;

    @AfterEach
    void tearDown() {
        if (consumer != null) consumer.close();
    }

    @Test
    void relay_publishesUnpublishedRowAndMarksItPublished() {
        OutboxJpaEntity row = new OutboxJpaEntity(
                UUID.randomUUID(), "auction-1", "AuctionCreated", "{\"auctionId\":\"auction-1\"}", Instant.now());
        outboxJpaRepository.save(row);

        outboxRelayJob.relayPendingEvents();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of("auction-events"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertThat(records.count()).isEqualTo(1);
        ConsumerRecord<String, String> record = records.iterator().next();
        assertThat(record.key()).isEqualTo("auction-1");

        OutboxJpaEntity reloaded = outboxJpaRepository.findById(row.getId()).orElseThrow();
        assertThat(reloaded.getPublishedAt()).isNotNull();
    }
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd /c/FPT/auction-service && mvn -Dtest=OutboxRelayJobTest test`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/resources/db/migration/V3__create_outbox_table.sql \
  src/main/java/com/nexus/auction/infrastructure/persistence/entity/OutboxJpaEntity.java \
  src/main/java/com/nexus/auction/infrastructure/persistence/OutboxJpaRepository.java \
  src/main/java/com/nexus/auction/application/port/out/EventPublisherPort.java \
  src/main/java/com/nexus/auction/infrastructure/messaging/OutboxEventPublisherAdapter.java \
  src/main/java/com/nexus/auction/infrastructure/messaging/OutboxRelayJob.java \
  src/test/java/com/nexus/auction/infrastructure/messaging/OutboxRelayJobTest.java
git commit -m "feat: add outbox table and OutboxRelayJob"
```

---

### Task 10: `CreateAuctionUseCase` + `SecurityConfig`/`UseCaseConfig` bootstrap

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/application/usecase/CreateAuctionCommand.java`, `AuctionResult.java`, `CreateAuctionUseCase.java`, `auction-service/src/main/java/com/nexus/auction/application/exception/AuctionNotFoundException.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/config/SecurityConfig.java`, `UseCaseConfig.java`
- Test: `auction-service/src/test/java/com/nexus/auction/application/usecase/CreateAuctionUseCaseTest.java`, `auction-service/src/test/java/com/nexus/auction/application/usecase/CreateAuctionUseCaseIntegrationTest.java`

**Interfaces:**
- Consumes: `AuctionRepositoryPort` (Task 7), `EventPublisherPort` (Task 9), `com.nexus.common.events.AuctionCreatedEvent` (Task 2), `com.nexus.common.core.exception.ConflictException`/`ValidationException` (existing).
- Produces: `CreateAuctionUseCase.create(CreateAuctionCommand)` returns `AuctionResult` — the `AuctionResult` record is reused by every later use case/controller task. `SecurityConfig` (public GETs under `/api/v1/**`, JWT filter, stateless) and `UseCaseConfig` (`@Bean` wiring) are the shared infra every later use case task adds a bean to.

- [ ] **Step 1: Write `AuctionNotFoundException`**

```java
package com.nexus.auction.application.exception;

import com.nexus.common.core.exception.NotFoundException;

public class AuctionNotFoundException extends NotFoundException {
    public AuctionNotFoundException(String id) {
        super("AUCTION_NOT_FOUND", "Auction not found: " + id);
    }
}
```

- [ ] **Step 2: Write `CreateAuctionCommand` and `AuctionResult`**

```java
package com.nexus.auction.application.usecase;

import java.math.BigDecimal;
import java.time.Instant;

public record CreateAuctionCommand(String productId, String sellerId, BigDecimal startingPrice,
                                    BigDecimal bidIncrement, Instant startTime, Instant endTime) {
}
```

```java
package com.nexus.auction.application.usecase;

import java.math.BigDecimal;
import java.time.Instant;

public record AuctionResult(String id, String productId, String sellerId, BigDecimal startingPrice,
                             BigDecimal bidIncrement, BigDecimal currentHighestBid, String currentHighestBidderId,
                             String status, Instant startTime, Instant endTime, int extensionCount,
                             String winnerId, BigDecimal finalPrice, Instant paymentDeadline) {
}
```

- [ ] **Step 3: Write the failing unit test**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.common.core.exception.ConflictException;
import com.nexus.common.core.exception.ValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CreateAuctionUseCaseTest {

    private AuctionRepositoryPort auctionRepositoryPort;
    private EventPublisherPort eventPublisherPort;
    private CreateAuctionUseCase useCase;

    @BeforeEach
    void setUp() {
        auctionRepositoryPort = mock(AuctionRepositoryPort.class);
        eventPublisherPort = mock(EventPublisherPort.class);
        useCase = new CreateAuctionUseCase(auctionRepositoryPort, eventPublisherPort);

        when(auctionRepositoryPort.save(any(Auction.class))).thenAnswer(inv -> inv.getArgument(0));
        when(auctionRepositoryPort.existsActiveOrPendingForProduct(any())).thenReturn(false);
        when(auctionRepositoryPort.countActiveOrPendingBySeller(any())).thenReturn(0L);
    }

    private CreateAuctionCommand validCommand() {
        Instant start = Instant.now().plus(1, ChronoUnit.HOURS);
        return new CreateAuctionCommand("product-1", "seller-1", new BigDecimal("100.00"),
                new BigDecimal("10.00"), start, start.plus(2, ChronoUnit.HOURS));
    }

    @Test
    void create_savesAuctionAndPublishesEvent() {
        AuctionResult result = useCase.create(validCommand());

        assertThat(result.status()).isEqualTo("PENDING");
        assertThat(result.productId()).isEqualTo("product-1");
        verify(eventPublisherPort).publish(any());
    }

    @Test
    void create_rejectsEndTimeBeforeStartTime() {
        Instant start = Instant.now().plus(1, ChronoUnit.HOURS);
        CreateAuctionCommand invalid = new CreateAuctionCommand("product-1", "seller-1",
                new BigDecimal("100.00"), new BigDecimal("10.00"), start, start.minus(1, ChronoUnit.HOURS));

        assertThatThrownBy(() -> useCase.create(invalid)).isInstanceOf(ValidationException.class);
        verify(auctionRepositoryPort, never()).save(any());
    }

    @Test
    void create_rejectsDurationBelowMinimum() {
        Instant start = Instant.now().plus(1, ChronoUnit.HOURS);
        CreateAuctionCommand tooShort = new CreateAuctionCommand("product-1", "seller-1",
                new BigDecimal("100.00"), new BigDecimal("10.00"), start, start.plus(30, ChronoUnit.MINUTES));

        assertThatThrownBy(() -> useCase.create(tooShort)).isInstanceOf(ValidationException.class);
    }

    @Test
    void create_rejectsDurationAboveMaximum() {
        Instant start = Instant.now().plus(1, ChronoUnit.HOURS);
        CreateAuctionCommand tooLong = new CreateAuctionCommand("product-1", "seller-1",
                new BigDecimal("100.00"), new BigDecimal("10.00"), start, start.plus(169, ChronoUnit.HOURS));

        assertThatThrownBy(() -> useCase.create(tooLong)).isInstanceOf(ValidationException.class);
    }

    @Test
    void create_rejectsWhenProductAlreadyHasAnActiveOrPendingAuction() {
        when(auctionRepositoryPort.existsActiveOrPendingForProduct("product-1")).thenReturn(true);

        assertThatThrownBy(() -> useCase.create(validCommand())).isInstanceOf(ConflictException.class);
        verify(auctionRepositoryPort, never()).save(any());
    }

    @Test
    void create_rejectsWhenSellerAtMaxActiveAuctions() {
        when(auctionRepositoryPort.countActiveOrPendingBySeller("seller-1")).thenReturn(5L);

        assertThatThrownBy(() -> useCase.create(validCommand())).isInstanceOf(ConflictException.class);
        verify(auctionRepositoryPort, never()).save(any());
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd /c/FPT/auction-service && mvn -Dtest=CreateAuctionUseCaseTest test`
Expected: FAIL with "cannot find symbol: class CreateAuctionUseCase".

- [ ] **Step 5: Write `CreateAuctionUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.common.core.FieldError;
import com.nexus.common.core.exception.ConflictException;
import com.nexus.common.core.exception.ValidationException;
import com.nexus.common.events.AuctionCreatedEvent;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;

public class CreateAuctionUseCase {

    private static final long MIN_DURATION_MINUTES = 60;
    private static final long MAX_DURATION_HOURS = 168;
    private static final int MAX_ACTIVE_AUCTIONS_PER_SELLER = 5;

    private final AuctionRepositoryPort auctionRepositoryPort;
    private final EventPublisherPort eventPublisherPort;

    public CreateAuctionUseCase(AuctionRepositoryPort auctionRepositoryPort, EventPublisherPort eventPublisherPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
        this.eventPublisherPort = eventPublisherPort;
    }

    @Transactional
    public AuctionResult create(CreateAuctionCommand command) {
        validateWindow(command);

        if (auctionRepositoryPort.existsActiveOrPendingForProduct(command.productId())) {
            throw new ConflictException("PRODUCT_ALREADY_IN_AUCTION",
                    "Product already has an active or pending auction: " + command.productId());
        }
        if (auctionRepositoryPort.countActiveOrPendingBySeller(command.sellerId()) >= MAX_ACTIVE_AUCTIONS_PER_SELLER) {
            throw new ConflictException("TOO_MANY_ACTIVE_AUCTIONS",
                    "Seller already has " + MAX_ACTIVE_AUCTIONS_PER_SELLER + " active or pending auctions");
        }

        Auction auction = Auction.create(command.productId(), command.sellerId(), command.startingPrice(),
                command.bidIncrement(), command.startTime(), command.endTime());
        Auction saved = auctionRepositoryPort.save(auction);

        eventPublisherPort.publish(new AuctionCreatedEvent(saved.getId(), saved.getProductId(), saved.getSellerId()));

        return toResult(saved);
    }

    private void validateWindow(CreateAuctionCommand command) {
        if (!command.startTime().isBefore(command.endTime())) {
            throw new ValidationException(List.of(
                    new FieldError("endTime", "endTime must be after startTime")));
        }
        Duration duration = Duration.between(command.startTime(), command.endTime());
        if (duration.toMinutes() < MIN_DURATION_MINUTES) {
            throw new ValidationException(List.of(new FieldError("endTime",
                    "Auction duration must be at least " + MIN_DURATION_MINUTES + " minutes")));
        }
        if (duration.toHours() > MAX_DURATION_HOURS) {
            throw new ValidationException(List.of(new FieldError("endTime",
                    "Auction duration must not exceed " + MAX_DURATION_HOURS + " hours")));
        }
    }

    static AuctionResult toResult(Auction a) {
        return new AuctionResult(a.getId(), a.getProductId(), a.getSellerId(), a.getStartingPrice(),
                a.getBidIncrement(), a.getCurrentHighestBid(), a.getCurrentHighestBidderId(), a.getStatus().name(),
                a.getStartTime(), a.getEndTime(), a.getExtensionCount(), a.getWinnerId(), a.getFinalPrice(),
                a.getPaymentDeadline());
    }
}
```

- [ ] **Step 6: Write `SecurityConfig` (copied verbatim from `catalog-service`, package renamed)**

```java
package com.nexus.auction.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.common.core.ApiError;
import com.nexus.common.core.ApiResponse;
import com.nexus.common.security.JwtAuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.util.List;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private final JwtAuthenticationFilter jwtAuthenticationFilter;

    public SecurityConfig(JwtAuthenticationFilter jwtAuthenticationFilter) {
        this.jwtAuthenticationFilter = jwtAuthenticationFilter;
    }

    @Bean
    public AuthenticationEntryPoint authenticationEntryPoint(ObjectMapper objectMapper) {
        return (request, response, authException) -> {
            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            ApiError error = new ApiError("UNAUTHENTICATED", "Authentication required", List.of());
            objectMapper.writeValue(response.getWriter(), ApiResponse.error(error));
        };
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http, AuthenticationEntryPoint authenticationEntryPoint) throws Exception {
        http.csrf(csrf -> csrf.disable())
            .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .exceptionHandling(handling -> handling.authenticationEntryPoint(authenticationEntryPoint))
            .authorizeHttpRequests(auth -> auth
                    .requestMatchers(HttpMethod.GET, "/api/v1/**").permitAll()
                    .requestMatchers("/actuator/**").permitAll()
                    .anyRequest().authenticated())
            .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }
}
```

- [ ] **Step 7: Write `UseCaseConfig` (first bean; later tasks add more `@Bean` methods to this same file)**

```java
package com.nexus.auction.infrastructure.config;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.application.usecase.CreateAuctionUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UseCaseConfig {

    @Bean
    public CreateAuctionUseCase createAuctionUseCase(AuctionRepositoryPort auctionPort,
                                                       EventPublisherPort eventPublisherPort) {
        return new CreateAuctionUseCase(auctionPort, eventPublisherPort);
    }
}
```

- [ ] **Step 8: Run the unit test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=CreateAuctionUseCaseTest test`
Expected: PASS (6 tests).

- [ ] **Step 9: Write the integration test (Testcontainers, proves the outbox write commits in the same transaction — mirrors `catalog-service`'s `CreateProductUseCaseIntegrationTest`)**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.infrastructure.persistence.OutboxJpaRepository;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class CreateAuctionUseCaseIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("auction_db").withUsername("nexus").withPassword("nexus");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eureka.client.enabled", () -> "false");
    }

    @Autowired private CreateAuctionUseCase createAuctionUseCase;
    @Autowired private AuctionRepositoryPort auctionRepositoryPort;
    @Autowired private OutboxJpaRepository outboxJpaRepository;

    private CreateAuctionCommand command() {
        Instant start = Instant.now().plus(1, ChronoUnit.HOURS);
        return new CreateAuctionCommand("11111111-1111-1111-1111-111111111111", "seller-1",
                new BigDecimal("100.00"), new BigDecimal("10.00"), start, start.plus(2, ChronoUnit.HOURS));
    }

    @Test
    void create_persistsAuctionAndAnUnpublishedOutboxRow() {
        AuctionResult result = createAuctionUseCase.create(command());

        assertThat(auctionRepositoryPort.findById(result.id())).isPresent();
        assertThat(outboxJpaRepository.findAll())
                .anySatisfy(row -> {
                    assertThat(row.getAggregateId()).isEqualTo(result.id());
                    assertThat(row.getEventType()).isEqualTo("AuctionCreated");
                    assertThat(row.getPublishedAt()).isNull();
                });
    }

    @Nested
    @Import(WhenOutboxWriteFails.FailingEventPublisherConfig.class)
    class WhenOutboxWriteFails {

        @Autowired private CreateAuctionUseCase createAuctionUseCase;
        @Autowired private JdbcTemplate jdbcTemplate;

        @Test
        void create_rollsBackTheAuctionInsertWhenTheOutboxWriteThrows() {
            long before = count();

            assertThatThrownBy(() -> createAuctionUseCase.create(command()))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("simulated outbox failure");

            assertThat(count()).isEqualTo(before);
        }

        private long count() {
            return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM auctions", Long.class);
        }

        @TestConfiguration
        static class FailingEventPublisherConfig {
            @Bean
            @Primary
            EventPublisherPort failingEventPublisherPort() {
                return event -> {
                    throw new RuntimeException("simulated outbox failure");
                };
            }
        }
    }
}
```

- [ ] **Step 10: Run the tests to verify they pass**

Run: `cd /c/FPT/auction-service && mvn -Dtest=CreateAuctionUseCaseIntegrationTest test`
Expected: PASS (2 tests).

- [ ] **Step 11: Commit**

```bash
git add src/main/java/com/nexus/auction/application/exception/AuctionNotFoundException.java \
  src/main/java/com/nexus/auction/application/usecase/CreateAuctionCommand.java \
  src/main/java/com/nexus/auction/application/usecase/AuctionResult.java \
  src/main/java/com/nexus/auction/application/usecase/CreateAuctionUseCase.java \
  src/main/java/com/nexus/auction/infrastructure/config/SecurityConfig.java \
  src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java \
  src/test/java/com/nexus/auction/application/usecase/CreateAuctionUseCaseTest.java \
  src/test/java/com/nexus/auction/application/usecase/CreateAuctionUseCaseIntegrationTest.java
git commit -m "feat: add CreateAuctionUseCase, SecurityConfig, UseCaseConfig"
```

---

### Task 11: `AuctionOwnershipPolicy` + `UpdateAuctionUseCase`

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/application/usecase/AuctionOwnershipPolicy.java`, `auction-service/src/main/java/com/nexus/auction/application/usecase/UpdateAuctionUseCase.java`
- Modify: `auction-service/src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java` (add `updateAuctionUseCase` bean)
- Test: `auction-service/src/test/java/com/nexus/auction/application/usecase/UpdateAuctionUseCaseTest.java`

**Interfaces:**
- Consumes: `AuctionRepositoryPort` (Task 7), `AuctionResult`/`toResult` (Task 10), `com.nexus.common.core.exception.ForbiddenException`/`ConflictException` (existing).
- Produces: `UpdateAuctionUseCase.update(String id, BigDecimal startingPrice, BigDecimal bidIncrement, Instant startTime, Instant endTime, String callerId)` — consumed by Task 15's `AuctionController`. `AuctionOwnershipPolicy.requireOwner(Auction, String callerId)` — reused by Task 12.

- [ ] **Step 1: Write the failing test**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.exception.AuctionNotFoundException;
import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.core.exception.ConflictException;
import com.nexus.common.core.exception.ForbiddenException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class UpdateAuctionUseCaseTest {

    private AuctionRepositoryPort auctionRepositoryPort;
    private UpdateAuctionUseCase useCase;

    @BeforeEach
    void setUp() {
        auctionRepositoryPort = mock(AuctionRepositoryPort.class);
        useCase = new UpdateAuctionUseCase(auctionRepositoryPort);
        when(auctionRepositoryPort.save(any(Auction.class))).thenAnswer(inv -> inv.getArgument(0));
    }

    private Auction pendingAuction() {
        Instant start = Instant.now().plus(1, ChronoUnit.HOURS);
        return Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                start, start.plus(2, ChronoUnit.HOURS));
    }

    @Test
    void update_updatesPriceIncrementAndWindowWhilePending() {
        Auction auction = pendingAuction();
        when(auctionRepositoryPort.findById(auction.getId())).thenReturn(Optional.of(auction));
        Instant newStart = Instant.now().plus(3, ChronoUnit.HOURS);

        AuctionResult result = useCase.update(auction.getId(), new BigDecimal("150.00"), new BigDecimal("15.00"),
                newStart, newStart.plus(2, ChronoUnit.HOURS), "seller-1");

        assertThat(result.startingPrice()).isEqualByComparingTo("150.00");
        assertThat(result.bidIncrement()).isEqualByComparingTo("15.00");
    }

    @Test
    void update_rejectsNonOwner() {
        Auction auction = pendingAuction();
        when(auctionRepositoryPort.findById(auction.getId())).thenReturn(Optional.of(auction));

        assertThatThrownBy(() -> useCase.update(auction.getId(), new BigDecimal("150.00"),
                new BigDecimal("15.00"), auction.getStartTime(), auction.getEndTime(), "other-seller"))
                .isInstanceOf(ForbiddenException.class);
    }

    @Test
    void update_rejectsWhenAuctionNotPending() {
        Auction active = pendingAuction().withStatus(AuctionStatus.ACTIVE);
        when(auctionRepositoryPort.findById(active.getId())).thenReturn(Optional.of(active));

        assertThatThrownBy(() -> useCase.update(active.getId(), new BigDecimal("150.00"),
                new BigDecimal("15.00"), active.getStartTime(), active.getEndTime(), "seller-1"))
                .isInstanceOf(ConflictException.class);
    }

    @Test
    void update_throwsNotFoundForUnknownId() {
        when(auctionRepositoryPort.findById("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> useCase.update("missing", new BigDecimal("150.00"),
                new BigDecimal("15.00"), Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS), "seller-1"))
                .isInstanceOf(AuctionNotFoundException.class);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/FPT/auction-service && mvn -Dtest=UpdateAuctionUseCaseTest test`
Expected: FAIL with "cannot find symbol: class UpdateAuctionUseCase".

- [ ] **Step 3: Write `AuctionOwnershipPolicy`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.domain.model.Auction;
import com.nexus.common.core.exception.ForbiddenException;

final class AuctionOwnershipPolicy {

    private AuctionOwnershipPolicy() {
    }

    static void requireOwner(Auction auction, String callerId) {
        if (!auction.getSellerId().equals(callerId)) {
            throw new ForbiddenException("AUCTION_NOT_OWNED",
                    "You can only modify your own auctions: " + auction.getId());
        }
    }
}
```

- [ ] **Step 4: Write `UpdateAuctionUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.exception.AuctionNotFoundException;
import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.core.exception.ConflictException;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;

public class UpdateAuctionUseCase {

    private final AuctionRepositoryPort auctionRepositoryPort;

    public UpdateAuctionUseCase(AuctionRepositoryPort auctionRepositoryPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
    }

    @Transactional
    public AuctionResult update(String id, BigDecimal startingPrice, BigDecimal bidIncrement,
                                 Instant startTime, Instant endTime, String callerId) {
        Auction existing = auctionRepositoryPort.findById(id).orElseThrow(() -> new AuctionNotFoundException(id));
        AuctionOwnershipPolicy.requireOwner(existing, callerId);
        if (existing.getStatus() != AuctionStatus.PENDING) {
            throw new ConflictException("AUCTION_NOT_EDITABLE",
                    "Only a pending auction can be updated: " + id);
        }

        Auction updated = auctionRepositoryPort.save(existing.withDetails(startingPrice, bidIncrement, startTime, endTime));
        return CreateAuctionUseCase.toResult(updated);
    }
}
```

- [ ] **Step 5: Add the bean to `UseCaseConfig`**

```java
    @Bean
    public UpdateAuctionUseCase updateAuctionUseCase(AuctionRepositoryPort auctionPort) {
        return new UpdateAuctionUseCase(auctionPort);
    }
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=UpdateAuctionUseCaseTest test`
Expected: PASS (4 tests).

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/nexus/auction/application/usecase/AuctionOwnershipPolicy.java \
  src/main/java/com/nexus/auction/application/usecase/UpdateAuctionUseCase.java \
  src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java \
  src/test/java/com/nexus/auction/application/usecase/UpdateAuctionUseCaseTest.java
git commit -m "feat: add UpdateAuctionUseCase and AuctionOwnershipPolicy"
```

---

### Task 12: `CancelAuctionUseCase` + `AdminCancelAuctionUseCase`

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/application/usecase/CancelAuctionUseCase.java`, `auction-service/src/main/java/com/nexus/auction/application/usecase/AdminCancelAuctionUseCase.java`
- Modify: `auction-service/src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java`
- Test: `auction-service/src/test/java/com/nexus/auction/application/usecase/CancelAuctionUseCaseTest.java`, `auction-service/src/test/java/com/nexus/auction/application/usecase/AdminCancelAuctionUseCaseTest.java`

**Interfaces:**
- Consumes: `AuctionRepositoryPort` (Task 7), `EventPublisherPort` (Task 9), `AuctionOwnershipPolicy` (Task 11), `com.nexus.common.events.AuctionCancelledEvent` (Task 2).
- Produces: `CancelAuctionUseCase.cancel(String id, String callerId)`, `AdminCancelAuctionUseCase.cancel(String id, String callerId)` — both consumed by Task 15's `AuctionController`.

- [ ] **Step 1: Write the failing tests**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.core.exception.ConflictException;
import com.nexus.common.core.exception.ForbiddenException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CancelAuctionUseCaseTest {

    private AuctionRepositoryPort auctionRepositoryPort;
    private EventPublisherPort eventPublisherPort;
    private CancelAuctionUseCase useCase;

    @BeforeEach
    void setUp() {
        auctionRepositoryPort = mock(AuctionRepositoryPort.class);
        eventPublisherPort = mock(EventPublisherPort.class);
        useCase = new CancelAuctionUseCase(auctionRepositoryPort, eventPublisherPort);
        when(auctionRepositoryPort.save(any(Auction.class))).thenAnswer(inv -> inv.getArgument(0));
    }

    private Auction newAuction() {
        Instant start = Instant.now().minus(1, ChronoUnit.HOURS);
        return Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                start, start.plus(2, ChronoUnit.HOURS));
    }

    @Test
    void cancel_allowsSellerToCancelWhilePending() {
        Auction auction = newAuction();
        when(auctionRepositoryPort.findById(auction.getId())).thenReturn(Optional.of(auction));

        AuctionResult result = useCase.cancel(auction.getId(), "seller-1");

        assertThat(result.status()).isEqualTo("CANCELLED");
        verify(eventPublisherPort).publish(any());
    }

    @Test
    void cancel_allowsSellerToCancelWhileActiveWithZeroBids() {
        Auction auction = newAuction().withStatus(AuctionStatus.ACTIVE);
        when(auctionRepositoryPort.findById(auction.getId())).thenReturn(Optional.of(auction));

        AuctionResult result = useCase.cancel(auction.getId(), "seller-1");

        assertThat(result.status()).isEqualTo("CANCELLED");
    }

    @Test
    void cancel_rejectsSellerOnceAuctionHasABid() {
        Auction auction = newAuction().withStatus(AuctionStatus.ACTIVE).withBid("bidder-1", new BigDecimal("100.00"));
        when(auctionRepositoryPort.findById(auction.getId())).thenReturn(Optional.of(auction));

        assertThatThrownBy(() -> useCase.cancel(auction.getId(), "seller-1"))
                .isInstanceOf(ConflictException.class);
    }

    @Test
    void cancel_rejectsNonOwner() {
        Auction auction = newAuction();
        when(auctionRepositoryPort.findById(auction.getId())).thenReturn(Optional.of(auction));

        assertThatThrownBy(() -> useCase.cancel(auction.getId(), "other-seller"))
                .isInstanceOf(ForbiddenException.class);
    }
}
```

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.core.exception.ConflictException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdminCancelAuctionUseCaseTest {

    private AuctionRepositoryPort auctionRepositoryPort;
    private EventPublisherPort eventPublisherPort;
    private AdminCancelAuctionUseCase useCase;

    @BeforeEach
    void setUp() {
        auctionRepositoryPort = mock(AuctionRepositoryPort.class);
        eventPublisherPort = mock(EventPublisherPort.class);
        useCase = new AdminCancelAuctionUseCase(auctionRepositoryPort, eventPublisherPort);
        when(auctionRepositoryPort.save(any(Auction.class))).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    void cancel_cancelsActiveAuctionWithBidsRegardlessOfOwnership() {
        Instant start = Instant.now().minus(1, ChronoUnit.HOURS);
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                        start, start.plus(2, ChronoUnit.HOURS))
                .withStatus(AuctionStatus.ACTIVE).withBid("bidder-1", new BigDecimal("100.00"));
        when(auctionRepositoryPort.findById(auction.getId())).thenReturn(Optional.of(auction));

        AuctionResult result = useCase.cancel(auction.getId(), "admin-1");

        assertThat(result.status()).isEqualTo("CANCELLED");
        verify(eventPublisherPort).publish(any());
    }

    @Test
    void cancel_rejectsWhenAlreadyEnded() {
        Instant start = Instant.now().minus(3, ChronoUnit.HOURS);
        Auction ended = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                        start, start.plus(1, ChronoUnit.HOURS))
                .withStatus(AuctionStatus.ENDED);
        when(auctionRepositoryPort.findById(ended.getId())).thenReturn(Optional.of(ended));

        assertThatThrownBy(() -> useCase.cancel(ended.getId(), "admin-1"))
                .isInstanceOf(ConflictException.class);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /c/FPT/auction-service && mvn -Dtest=CancelAuctionUseCaseTest,AdminCancelAuctionUseCaseTest test`
Expected: FAIL — classes don't exist yet.

- [ ] **Step 3: Write `CancelAuctionUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.exception.AuctionNotFoundException;
import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.core.exception.ConflictException;
import com.nexus.common.events.AuctionCancelledEvent;
import org.springframework.transaction.annotation.Transactional;

public class CancelAuctionUseCase {

    private final AuctionRepositoryPort auctionRepositoryPort;
    private final EventPublisherPort eventPublisherPort;

    public CancelAuctionUseCase(AuctionRepositoryPort auctionRepositoryPort, EventPublisherPort eventPublisherPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
        this.eventPublisherPort = eventPublisherPort;
    }

    @Transactional
    public AuctionResult cancel(String id, String callerId) {
        Auction existing = auctionRepositoryPort.findById(id).orElseThrow(() -> new AuctionNotFoundException(id));
        AuctionOwnershipPolicy.requireOwner(existing, callerId);

        boolean eligible = existing.getStatus() == AuctionStatus.PENDING
                || (existing.getStatus() == AuctionStatus.ACTIVE && existing.getCurrentHighestBid() == null);
        if (!eligible) {
            throw new ConflictException("AUCTION_NOT_CANCELLABLE",
                    "Only a pending auction, or an active auction with no bids, can be cancelled by its seller: " + id);
        }

        Auction cancelled = auctionRepositoryPort.save(existing.withStatus(AuctionStatus.CANCELLED));
        eventPublisherPort.publish(new AuctionCancelledEvent(cancelled.getId(), callerId));
        return CreateAuctionUseCase.toResult(cancelled);
    }
}
```

- [ ] **Step 4: Write `AdminCancelAuctionUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.exception.AuctionNotFoundException;
import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.core.exception.ConflictException;
import com.nexus.common.events.AuctionCancelledEvent;
import org.springframework.transaction.annotation.Transactional;

public class AdminCancelAuctionUseCase {

    private final AuctionRepositoryPort auctionRepositoryPort;
    private final EventPublisherPort eventPublisherPort;

    public AdminCancelAuctionUseCase(AuctionRepositoryPort auctionRepositoryPort, EventPublisherPort eventPublisherPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
        this.eventPublisherPort = eventPublisherPort;
    }

    @Transactional
    public AuctionResult cancel(String id, String callerId) {
        Auction existing = auctionRepositoryPort.findById(id).orElseThrow(() -> new AuctionNotFoundException(id));
        if (existing.getStatus() == AuctionStatus.ENDED || existing.getStatus() == AuctionStatus.CANCELLED) {
            throw new ConflictException("AUCTION_NOT_CANCELLABLE",
                    "Cannot cancel an auction that has already ended or been cancelled: " + id);
        }

        Auction cancelled = auctionRepositoryPort.save(existing.withStatus(AuctionStatus.CANCELLED));
        eventPublisherPort.publish(new AuctionCancelledEvent(cancelled.getId(), callerId));
        return CreateAuctionUseCase.toResult(cancelled);
    }
}
```

- [ ] **Step 5: Add both beans to `UseCaseConfig`**

```java
    @Bean
    public CancelAuctionUseCase cancelAuctionUseCase(AuctionRepositoryPort auctionPort,
                                                       EventPublisherPort eventPublisherPort) {
        return new CancelAuctionUseCase(auctionPort, eventPublisherPort);
    }

    @Bean
    public AdminCancelAuctionUseCase adminCancelAuctionUseCase(AuctionRepositoryPort auctionPort,
                                                                 EventPublisherPort eventPublisherPort) {
        return new AdminCancelAuctionUseCase(auctionPort, eventPublisherPort);
    }
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd /c/FPT/auction-service && mvn -Dtest=CancelAuctionUseCaseTest,AdminCancelAuctionUseCaseTest test`
Expected: PASS (6 tests total).

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/nexus/auction/application/usecase/CancelAuctionUseCase.java \
  src/main/java/com/nexus/auction/application/usecase/AdminCancelAuctionUseCase.java \
  src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java \
  src/test/java/com/nexus/auction/application/usecase/CancelAuctionUseCaseTest.java \
  src/test/java/com/nexus/auction/application/usecase/AdminCancelAuctionUseCaseTest.java
git commit -m "feat: add CancelAuctionUseCase and AdminCancelAuctionUseCase"
```

---

### Task 13: `PlaceBidUseCase` (pessimistic lock, anti-sniping, events) + concurrency integration test

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/application/usecase/BidResult.java`, `auction-service/src/main/java/com/nexus/auction/application/usecase/PlaceBidUseCase.java`
- Modify: `auction-service/src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java`
- Test: `auction-service/src/test/java/com/nexus/auction/application/usecase/PlaceBidUseCaseTest.java`, `auction-service/src/test/java/com/nexus/auction/application/usecase/PlaceBidUseCaseConcurrencyIntegrationTest.java`

**Interfaces:**
- Consumes: `AuctionRepositoryPort.findByIdForUpdate` (Task 7), `BidRepositoryPort` (Task 8), `EventPublisherPort` (Task 9), `BidValidationPolicy` (Task 5), `AntiSnipingPolicy` (Task 6), `com.nexus.common.events.BidPlacedEvent`/`OutbidEvent` (Task 2).
- Produces: `PlaceBidUseCase.placeBid(String auctionId, String bidderId, BigDecimal amount)` returns `BidResult` — consumed by Task 15's `BidController`.

- [ ] **Step 1: Write `BidResult`**

```java
package com.nexus.auction.application.usecase;

import java.math.BigDecimal;
import java.time.Instant;

public record BidResult(String id, String auctionId, String bidderId, BigDecimal amount, Instant placedAt) {
}
```

- [ ] **Step 2: Write the failing unit test (mocked ports — proves the logic, not the locking)**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.exception.AuctionNotFoundException;
import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.BidRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.auction.domain.model.Bid;
import com.nexus.common.core.exception.ConflictException;
import com.nexus.common.core.exception.ValidationException;
import com.nexus.common.events.BidPlacedEvent;
import com.nexus.common.events.OutbidEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PlaceBidUseCaseTest {

    private AuctionRepositoryPort auctionRepositoryPort;
    private BidRepositoryPort bidRepositoryPort;
    private EventPublisherPort eventPublisherPort;
    private PlaceBidUseCase useCase;

    @BeforeEach
    void setUp() {
        auctionRepositoryPort = mock(AuctionRepositoryPort.class);
        bidRepositoryPort = mock(BidRepositoryPort.class);
        eventPublisherPort = mock(EventPublisherPort.class);
        useCase = new PlaceBidUseCase(auctionRepositoryPort, bidRepositoryPort, eventPublisherPort);
        when(auctionRepositoryPort.save(any(Auction.class))).thenAnswer(inv -> inv.getArgument(0));
        when(bidRepositoryPort.save(any(Bid.class))).thenAnswer(inv -> inv.getArgument(0));
    }

    private Auction activeAuction() {
        Instant start = Instant.now().minus(1, ChronoUnit.HOURS);
        return Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                        start, start.plus(2, ChronoUnit.HOURS))
                .withStatus(AuctionStatus.ACTIVE);
    }

    @Test
    void placeBid_savesBidAndUpdatesAuctionHighest() {
        Auction auction = activeAuction();
        when(auctionRepositoryPort.findByIdForUpdate(auction.getId())).thenReturn(Optional.of(auction));

        BidResult result = useCase.placeBid(auction.getId(), "bidder-1", new BigDecimal("100.00"));

        assertThat(result.amount()).isEqualByComparingTo("100.00");
        verify(auctionRepositoryPort).save(argThat(a -> a.getCurrentHighestBidderId().equals("bidder-1")));
        verify(eventPublisherPort).publish(any(BidPlacedEvent.class));
    }

    @Test
    void placeBid_publishesOutbidEventForThePreviousHighestBidder() {
        Auction auction = activeAuction().withBid("bidder-1", new BigDecimal("100.00"));
        when(auctionRepositoryPort.findByIdForUpdate(auction.getId())).thenReturn(Optional.of(auction));

        useCase.placeBid(auction.getId(), "bidder-2", new BigDecimal("110.00"));

        verify(eventPublisherPort).publish(argThat(event ->
                event instanceof OutbidEvent outbid && outbid.getOutbidBidderId().equals("bidder-1")));
    }

    @Test
    void placeBid_doesNotPublishOutbidWhenThereWasNoPreviousBidder() {
        Auction auction = activeAuction();
        when(auctionRepositoryPort.findByIdForUpdate(auction.getId())).thenReturn(Optional.of(auction));

        useCase.placeBid(auction.getId(), "bidder-1", new BigDecimal("100.00"));

        verify(eventPublisherPort, never()).publish(any(OutbidEvent.class));
    }

    @Test
    void placeBid_rejectsBidBelowMinimum() {
        Auction auction = activeAuction();
        when(auctionRepositoryPort.findByIdForUpdate(auction.getId())).thenReturn(Optional.of(auction));

        assertThatThrownBy(() -> useCase.placeBid(auction.getId(), "bidder-1", new BigDecimal("50.00")))
                .isInstanceOf(ValidationException.class);
        verify(bidRepositoryPort, never()).save(any());
    }

    @Test
    void placeBid_rejectsBidOnPendingAuction() {
        Auction pending = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                Instant.now().plus(1, ChronoUnit.HOURS), Instant.now().plus(2, ChronoUnit.HOURS));
        when(auctionRepositoryPort.findByIdForUpdate(pending.getId())).thenReturn(Optional.of(pending));

        assertThatThrownBy(() -> useCase.placeBid(pending.getId(), "bidder-1", new BigDecimal("100.00")))
                .isInstanceOf(ConflictException.class);
    }

    @Test
    void placeBid_throwsNotFoundForUnknownAuction() {
        when(auctionRepositoryPort.findByIdForUpdate("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> useCase.placeBid("missing", "bidder-1", new BigDecimal("100.00")))
                .isInstanceOf(AuctionNotFoundException.class);
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd /c/FPT/auction-service && mvn -Dtest=PlaceBidUseCaseTest test`
Expected: FAIL with "cannot find symbol: class PlaceBidUseCase".

- [ ] **Step 4: Write `PlaceBidUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.exception.AuctionNotFoundException;
import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.BidRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.Bid;
import com.nexus.auction.domain.service.AntiSnipingPolicy;
import com.nexus.auction.domain.service.BidValidationPolicy;
import com.nexus.common.events.BidPlacedEvent;
import com.nexus.common.events.OutbidEvent;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

public class PlaceBidUseCase {

    private final AuctionRepositoryPort auctionRepositoryPort;
    private final BidRepositoryPort bidRepositoryPort;
    private final EventPublisherPort eventPublisherPort;

    public PlaceBidUseCase(AuctionRepositoryPort auctionRepositoryPort, BidRepositoryPort bidRepositoryPort,
                            EventPublisherPort eventPublisherPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
        this.bidRepositoryPort = bidRepositoryPort;
        this.eventPublisherPort = eventPublisherPort;
    }

    // @Transactional is what makes findByIdForUpdate's SELECT ... FOR UPDATE actually hold the
    // row lock for the duration of this method: the lock is released at commit/rollback, so every
    // concurrent caller for the same auctionId blocks here until the previous one finishes,
    // serializing bid validation + write and eliminating the lost-update race.
    @Transactional
    public BidResult placeBid(String auctionId, String bidderId, BigDecimal amount) {
        Auction auction = auctionRepositoryPort.findByIdForUpdate(auctionId)
                .orElseThrow(() -> new AuctionNotFoundException(auctionId));

        BidValidationPolicy.validate(auction, amount);

        String previousHighestBidderId = auction.getCurrentHighestBidderId();

        Bid bid = Bid.create(auctionId, bidderId, amount);
        bidRepositoryPort.save(bid);

        Auction updated = auction.withBid(bidderId, amount);
        Optional<Instant> extendedEndTime = AntiSnipingPolicy.tryExtend(updated, Instant.now());
        if (extendedEndTime.isPresent()) {
            updated = updated.withExtendedEndTime(extendedEndTime.get());
        }
        auctionRepositoryPort.save(updated);

        eventPublisherPort.publish(new BidPlacedEvent(auctionId, bidderId, amount));
        if (previousHighestBidderId != null && !previousHighestBidderId.equals(bidderId)) {
            eventPublisherPort.publish(new OutbidEvent(auctionId, previousHighestBidderId, amount));
        }

        return new BidResult(bid.getId(), auctionId, bidderId, amount, bid.getPlacedAt());
    }
}
```

- [ ] **Step 5: Add the bean to `UseCaseConfig`**

```java
    @Bean
    public PlaceBidUseCase placeBidUseCase(AuctionRepositoryPort auctionPort, BidRepositoryPort bidPort,
                                            EventPublisherPort eventPublisherPort) {
        return new PlaceBidUseCase(auctionPort, bidPort, eventPublisherPort);
    }
```

- [ ] **Step 6: Run the unit test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=PlaceBidUseCaseTest test`
Expected: PASS (6 tests).

- [ ] **Step 7: Write the concurrency integration test — the Review Focus case for lost updates**

This is the test that actually proves the pessimistic lock works: 20 threads race to place the *same* bid amount on the *same* auction simultaneously. Without correct locking, more than one thread could read `current_highest_bid = null` before any of them writes, and more than one would then "succeed" — writing two `bids` rows at the same amount and leaving the auction's highest-bid fields in a state only one of them should have produced. With the lock, exactly one thread commits its read-validate-write as an atomic unit; every other thread's `findByIdForUpdate` blocks until the first commits, re-reads the now-updated highest bid, and correctly rejects itself (`amount` no longer `>= highest + increment` since it's equal, not greater).

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.infrastructure.persistence.entity.AuctionJpaEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class PlaceBidUseCaseConcurrencyIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("auction_db").withUsername("nexus").withPassword("nexus");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eureka.client.enabled", () -> "false");
        // Testcontainers' default connection pool (HikariCP, default max 10) would deadlock this
        // test: 20 threads each hold a connection blocked on the row lock, so a pool smaller than
        // the thread count starves the very threads waiting to acquire the lock.
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "25");
    }

    @Autowired private PlaceBidUseCase placeBidUseCase;
    @Autowired private AuctionRepositoryPort auctionRepositoryPort;
    @Autowired private JdbcTemplate jdbcTemplate;

    @Test
    void placeBid_underConcurrentIdenticalBids_exactlyOneWinsAndNoBidIsLost() throws InterruptedException {
        UUID auctionId = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO auctions (id, product_id, seller_id, starting_price, bid_increment, status,
                                       start_time, end_time)
                VALUES (?, ?, 'seller-1', 100.00, 10.00, 'ACTIVE', now() - interval '1 hour', now() + interval '1 hour')
                """, auctionId, UUID.randomUUID());

        int threadCount = 20;
        BigDecimal contestedAmount = new BigDecimal("500.00");
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger();
        List<Future<?>> futures = new java.util.ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            String bidderId = "bidder-" + i;
            futures.add(executor.submit(() -> {
                try {
                    startGate.await();
                    placeBidUseCase.placeBid(auctionId.toString(), bidderId, contestedAmount);
                    successCount.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception ignoredValidationOrConflict) {
                    // Expected for every thread except the one that wins the race.
                }
            }));
        }
        startGate.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertThat(successCount.get())
                .as("exactly one of the identical concurrent bids must be accepted")
                .isEqualTo(1);

        assertThat(auctionRepositoryPort.findById(auctionId.toString()).orElseThrow().getCurrentHighestBid())
                .as("the auction's recorded highest bid must equal the one bid that actually won")
                .isEqualByComparingTo(contestedAmount);

        Long bidRows = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM bids WHERE auction_id = ?", Long.class, auctionId);
        assertThat(bidRows)
                .as("no duplicate/lost-update bid row may exist at the contested amount")
                .isEqualTo(1L);
    }
}
```

(Note: this test does not import `AuctionJpaEntity` — it only needs `AuctionRepositoryPort`, `JdbcTemplate`, and the standard `java.util.concurrent`/`java.math` imports shown in the `import` block above.)

- [ ] **Step 8: Run the concurrency test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=PlaceBidUseCaseConcurrencyIntegrationTest test`
Expected: PASS. If it ever fails with `successCount.get() > 1`, that is a real regression in the locking strategy (e.g. someone switched `findByIdForUpdate` to a plain `findById`) — do not weaken the assertion to "make it pass."

- [ ] **Step 9: Commit**

```bash
git add src/main/java/com/nexus/auction/application/usecase/BidResult.java \
  src/main/java/com/nexus/auction/application/usecase/PlaceBidUseCase.java \
  src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java \
  src/test/java/com/nexus/auction/application/usecase/PlaceBidUseCaseTest.java \
  src/test/java/com/nexus/auction/application/usecase/PlaceBidUseCaseConcurrencyIntegrationTest.java
git commit -m "feat: add PlaceBidUseCase with pessimistic-lock concurrency control"
```

---

### Task 14: `GetAuctionUseCase` + `ListAuctionsUseCase` + `GetBidHistoryUseCase`

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/application/usecase/GetAuctionUseCase.java`, `AuctionSearchQuery.java`, `ListAuctionsUseCase.java`, `GetBidHistoryUseCase.java`
- Modify: `auction-service/src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java`
- Test: `auction-service/src/test/java/com/nexus/auction/application/usecase/GetAuctionUseCaseTest.java`, `ListAuctionsUseCaseTest.java`, `GetBidHistoryUseCaseTest.java`

**Interfaces:**
- Consumes: `AuctionRepositoryPort`/`BidRepositoryPort` (Tasks 7–8), `AuctionResult`/`toResult` (Task 10), `BidResult` (Task 13).
- Produces: three read use cases consumed by Task 15's `AuctionController`/`BidController`.

- [ ] **Step 1: Write the failing tests**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.exception.AuctionNotFoundException;
import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.domain.model.Auction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetAuctionUseCaseTest {

    private AuctionRepositoryPort auctionRepositoryPort;
    private GetAuctionUseCase useCase;

    @BeforeEach
    void setUp() {
        auctionRepositoryPort = mock(AuctionRepositoryPort.class);
        useCase = new GetAuctionUseCase(auctionRepositoryPort);
    }

    @Test
    void get_returnsTheAuction() {
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS));
        when(auctionRepositoryPort.findById(auction.getId())).thenReturn(Optional.of(auction));

        AuctionResult result = useCase.get(auction.getId());

        assertThat(result.id()).isEqualTo(auction.getId());
    }

    @Test
    void get_throwsNotFoundForUnknownId() {
        when(auctionRepositoryPort.findById("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> useCase.get("missing")).isInstanceOf(AuctionNotFoundException.class);
    }
}
```

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.domain.model.Auction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ListAuctionsUseCaseTest {

    private AuctionRepositoryPort auctionRepositoryPort;
    private ListAuctionsUseCase useCase;

    @BeforeEach
    void setUp() {
        auctionRepositoryPort = mock(AuctionRepositoryPort.class);
        useCase = new ListAuctionsUseCase(auctionRepositoryPort);
    }

    @Test
    void list_delegatesFiltersToTheRepository() {
        Auction auction = Auction.create("product-1", "seller-1", new BigDecimal("100.00"), new BigDecimal("10.00"),
                Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS));
        when(auctionRepositoryPort.search("ACTIVE", "seller-1", null, 0, 20)).thenReturn(List.of(auction));

        List<AuctionResult> results = useCase.list(new AuctionSearchQuery("ACTIVE", "seller-1", null, 0, 20));

        assertThat(results).hasSize(1);
        assertThat(results.get(0).id()).isEqualTo(auction.getId());
    }
}
```

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.BidRepositoryPort;
import com.nexus.auction.domain.model.Bid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetBidHistoryUseCaseTest {

    private BidRepositoryPort bidRepositoryPort;
    private GetBidHistoryUseCase useCase;

    @BeforeEach
    void setUp() {
        bidRepositoryPort = mock(BidRepositoryPort.class);
        useCase = new GetBidHistoryUseCase(bidRepositoryPort);
    }

    @Test
    void getHistory_returnsBidsForTheAuction() {
        Bid bid = Bid.create("auction-1", "bidder-1", new BigDecimal("100.00"));
        when(bidRepositoryPort.findByAuctionId("auction-1", 0, 20)).thenReturn(List.of(bid));

        List<BidResult> results = useCase.getHistory("auction-1", 0, 20);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).bidderId()).isEqualTo("bidder-1");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd /c/FPT/auction-service && mvn -Dtest=GetAuctionUseCaseTest,ListAuctionsUseCaseTest,GetBidHistoryUseCaseTest test`
Expected: FAIL — classes don't exist yet.

- [ ] **Step 3: Write `GetAuctionUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.exception.AuctionNotFoundException;
import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.domain.model.Auction;

public class GetAuctionUseCase {

    private final AuctionRepositoryPort auctionRepositoryPort;

    public GetAuctionUseCase(AuctionRepositoryPort auctionRepositoryPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
    }

    public AuctionResult get(String id) {
        Auction auction = auctionRepositoryPort.findById(id).orElseThrow(() -> new AuctionNotFoundException(id));
        return CreateAuctionUseCase.toResult(auction);
    }
}
```

- [ ] **Step 4: Write `AuctionSearchQuery` and `ListAuctionsUseCase`**

```java
package com.nexus.auction.application.usecase;

public record AuctionSearchQuery(String status, String sellerId, String productId, int page, int size) {
}
```

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;

import java.util.List;

public class ListAuctionsUseCase {

    private final AuctionRepositoryPort auctionRepositoryPort;

    public ListAuctionsUseCase(AuctionRepositoryPort auctionRepositoryPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
    }

    public List<AuctionResult> list(AuctionSearchQuery query) {
        return auctionRepositoryPort.search(query.status(), query.sellerId(), query.productId(),
                        query.page(), query.size())
                .stream().map(CreateAuctionUseCase::toResult).toList();
    }
}
```

- [ ] **Step 5: Write `GetBidHistoryUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.BidRepositoryPort;
import com.nexus.auction.domain.model.Bid;

import java.util.List;

public class GetBidHistoryUseCase {

    private final BidRepositoryPort bidRepositoryPort;

    public GetBidHistoryUseCase(BidRepositoryPort bidRepositoryPort) {
        this.bidRepositoryPort = bidRepositoryPort;
    }

    public List<BidResult> getHistory(String auctionId, int page, int size) {
        return bidRepositoryPort.findByAuctionId(auctionId, page, size).stream()
                .map(this::toResult).toList();
    }

    private BidResult toResult(Bid bid) {
        return new BidResult(bid.getId(), bid.getAuctionId(), bid.getBidderId(), bid.getAmount(), bid.getPlacedAt());
    }
}
```

- [ ] **Step 6: Add all three beans to `UseCaseConfig`**

```java
    @Bean
    public GetAuctionUseCase getAuctionUseCase(AuctionRepositoryPort auctionPort) {
        return new GetAuctionUseCase(auctionPort);
    }

    @Bean
    public ListAuctionsUseCase listAuctionsUseCase(AuctionRepositoryPort auctionPort) {
        return new ListAuctionsUseCase(auctionPort);
    }

    @Bean
    public GetBidHistoryUseCase getBidHistoryUseCase(BidRepositoryPort bidPort) {
        return new GetBidHistoryUseCase(bidPort);
    }
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd /c/FPT/auction-service && mvn -Dtest=GetAuctionUseCaseTest,ListAuctionsUseCaseTest,GetBidHistoryUseCaseTest test`
Expected: PASS (4 tests total).

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/nexus/auction/application/usecase/GetAuctionUseCase.java \
  src/main/java/com/nexus/auction/application/usecase/AuctionSearchQuery.java \
  src/main/java/com/nexus/auction/application/usecase/ListAuctionsUseCase.java \
  src/main/java/com/nexus/auction/application/usecase/GetBidHistoryUseCase.java \
  src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java \
  src/test/java/com/nexus/auction/application/usecase/GetAuctionUseCaseTest.java \
  src/test/java/com/nexus/auction/application/usecase/ListAuctionsUseCaseTest.java \
  src/test/java/com/nexus/auction/application/usecase/GetBidHistoryUseCaseTest.java
git commit -m "feat: add GetAuctionUseCase, ListAuctionsUseCase, GetBidHistoryUseCase"
```

---

### Task 15: `AuctionController` + `BidController` + DTOs + mapper

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/api/dto/request/CreateAuctionRequest.java`, `UpdateAuctionRequest.java`, `PlaceBidRequest.java`, `auction-service/src/main/java/com/nexus/auction/api/dto/response/AuctionResponse.java`, `BidResponse.java`, `auction-service/src/main/java/com/nexus/auction/api/mapper/AuctionApiMapper.java`, `auction-service/src/main/java/com/nexus/auction/api/AuctionController.java`, `auction-service/src/main/java/com/nexus/auction/api/BidController.java`
- Test: `auction-service/src/test/java/com/nexus/auction/api/AuctionControllerTest.java`, `auction-service/src/test/java/com/nexus/auction/api/BidControllerTest.java`

**Interfaces:**
- Consumes: every use case from Tasks 10–14, `SecurityConfig` (Task 10).
- Produces: the public HTTP surface — `/api/v1/auctions/**` — consumed by Task 19's gateway routing.

- [ ] **Step 1: Write the request/response DTOs**

```java
package com.nexus.auction.api.dto.request;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.time.Instant;

public record CreateAuctionRequest(
        @NotBlank String productId,
        @NotNull @DecimalMin(value = "0.01", message = "startingPrice must be greater than zero") BigDecimal startingPrice,
        @NotNull @DecimalMin(value = "0.01", message = "bidIncrement must be greater than zero") BigDecimal bidIncrement,
        @NotNull Instant startTime,
        @NotNull Instant endTime) {
}
```

```java
package com.nexus.auction.api.dto.request;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.time.Instant;

public record UpdateAuctionRequest(
        @NotNull @DecimalMin(value = "0.01", message = "startingPrice must be greater than zero") BigDecimal startingPrice,
        @NotNull @DecimalMin(value = "0.01", message = "bidIncrement must be greater than zero") BigDecimal bidIncrement,
        @NotNull Instant startTime,
        @NotNull Instant endTime) {
}
```

```java
package com.nexus.auction.api.dto.request;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record PlaceBidRequest(
        @NotNull @DecimalMin(value = "0.01", message = "amount must be greater than zero") BigDecimal amount) {
}
```

```java
package com.nexus.auction.api.dto.response;

import java.math.BigDecimal;
import java.time.Instant;

public record AuctionResponse(String id, String productId, String sellerId, BigDecimal startingPrice,
                               BigDecimal bidIncrement, BigDecimal currentHighestBid, String currentHighestBidderId,
                               String status, Instant startTime, Instant endTime, int extensionCount,
                               String winnerId, BigDecimal finalPrice, Instant paymentDeadline) {
}
```

```java
package com.nexus.auction.api.dto.response;

import java.math.BigDecimal;
import java.time.Instant;

public record BidResponse(String id, String auctionId, String bidderId, BigDecimal amount, Instant placedAt) {
}
```

- [ ] **Step 2: Write `AuctionApiMapper`**

```java
package com.nexus.auction.api.mapper;

import com.nexus.auction.api.dto.response.AuctionResponse;
import com.nexus.auction.api.dto.response.BidResponse;
import com.nexus.auction.application.usecase.AuctionResult;
import com.nexus.auction.application.usecase.BidResult;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface AuctionApiMapper {
    AuctionResponse toResponse(AuctionResult result);
    BidResponse toResponse(BidResult result);
}
```

- [ ] **Step 3: Write the failing controller tests**

```java
package com.nexus.auction.api;

import com.nexus.auction.api.mapper.AuctionApiMapperImpl;
import com.nexus.auction.application.usecase.*;
import com.nexus.auction.infrastructure.config.SecurityConfig;
import com.nexus.common.core.exception.ForbiddenException;
import com.nexus.common.security.JwtAuthenticationFilter;
import com.nexus.common.security.JwtTokenProvider;
import com.nexus.common.security.PrivilegeAuthorizationAspect;
import com.nexus.common.web.GlobalExceptionHandler;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(AuctionController.class)
@ImportAutoConfiguration(AopAutoConfiguration.class)
@Import({GlobalExceptionHandler.class, AuctionApiMapperImpl.class, SecurityConfig.class,
        JwtAuthenticationFilter.class, PrivilegeAuthorizationAspect.class})
class AuctionControllerTest {

    @Autowired private MockMvc mockMvc;

    @MockBean private CreateAuctionUseCase createAuctionUseCase;
    @MockBean private UpdateAuctionUseCase updateAuctionUseCase;
    @MockBean private CancelAuctionUseCase cancelAuctionUseCase;
    @MockBean private AdminCancelAuctionUseCase adminCancelAuctionUseCase;
    @MockBean private GetAuctionUseCase getAuctionUseCase;
    @MockBean private ListAuctionsUseCase listAuctionsUseCase;
    @MockBean private JwtTokenProvider jwtTokenProvider;

    private void authenticateAs(String subject, String... privileges) {
        when(jwtTokenProvider.isValid("good-token")).thenReturn(true);
        Claims claims = Jwts.claims().subject(subject).add("privileges", List.of(privileges)).build();
        when(jwtTokenProvider.parseClaims("good-token")).thenReturn(claims);
    }

    private AuctionResult sampleResult(String sellerId) {
        Instant start = Instant.now().plus(1, ChronoUnit.HOURS);
        return new AuctionResult("auction-id", "product-1", sellerId, new BigDecimal("100.00"),
                new BigDecimal("10.00"), null, null, "PENDING", start, start.plus(2, ChronoUnit.HOURS), 0,
                null, null, null);
    }

    @Test
    void create_returns401WithoutToken() throws Exception {
        mockMvc.perform(post("/api/v1/auctions")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"productId":"product-1","startingPrice":100.00,"bidIncrement":10.00,
                                 "startTime":"2026-10-01T00:00:00Z","endTime":"2026-10-02T00:00:00Z"}"""))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void create_ignoresSpoofedSellerId_usesJwtSubjectInstead() throws Exception {
        authenticateAs("seller-id", "AUCTION.CREATE");
        when(createAuctionUseCase.create(any())).thenReturn(sampleResult("seller-id"));

        mockMvc.perform(post("/api/v1/auctions")
                        .header("Authorization", "Bearer good-token")
                        .contentType(APPLICATION_JSON)
                        .content("""
                                {"productId":"product-1","sellerId":"attacker-id","startingPrice":100.00,
                                 "bidIncrement":10.00,"startTime":"2026-10-01T00:00:00Z","endTime":"2026-10-02T00:00:00Z"}"""))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.data.sellerId").value("seller-id"));

        verify(createAuctionUseCase).create(argThat(
                (CreateAuctionCommand cmd) -> cmd.sellerId().equals("seller-id")));
    }

    @Test
    void cancel_returns403WhenUseCaseRejectsNonOwner() throws Exception {
        authenticateAs("other-seller", "AUCTION.CANCEL");
        when(cancelAuctionUseCase.cancel("a-id", "other-seller"))
                .thenThrow(new ForbiddenException("AUCTION_NOT_OWNED", "not yours"));

        mockMvc.perform(delete("/api/v1/auctions/a-id").header("Authorization", "Bearer good-token"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("AUCTION_NOT_OWNED"));
    }

    @Test
    void adminCancel_returns403WithoutAdminCancelPrivilege() throws Exception {
        authenticateAs("seller-id", "AUCTION.CANCEL");

        mockMvc.perform(post("/api/v1/auctions/a-id/admin-cancel").header("Authorization", "Bearer good-token"))
                .andExpect(status().isForbidden());

        verifyNoInteractions(adminCancelAuctionUseCase);
    }

    @Test
    void get_isPublicAndReturns200WithoutAToken() throws Exception {
        when(getAuctionUseCase.get("a-id")).thenReturn(sampleResult("seller-id"));

        mockMvc.perform(get("/api/v1/auctions/a-id"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.id").value("auction-id"));
    }

    @Test
    void list_isPublicAndBindsFilters() throws Exception {
        when(listAuctionsUseCase.list(any())).thenReturn(List.of());

        mockMvc.perform(get("/api/v1/auctions")
                        .param("status", "ACTIVE").param("page", "1").param("size", "5"))
                .andExpect(status().isOk());

        org.mockito.ArgumentCaptor<AuctionSearchQuery> captor = org.mockito.ArgumentCaptor.forClass(AuctionSearchQuery.class);
        verify(listAuctionsUseCase).list(captor.capture());
        org.assertj.core.api.Assertions.assertThat(captor.getValue().status()).isEqualTo("ACTIVE");
        org.assertj.core.api.Assertions.assertThat(captor.getValue().page()).isEqualTo(1);
    }
}
```

```java
package com.nexus.auction.api;

import com.nexus.auction.api.mapper.AuctionApiMapperImpl;
import com.nexus.auction.application.usecase.BidResult;
import com.nexus.auction.application.usecase.GetBidHistoryUseCase;
import com.nexus.auction.application.usecase.PlaceBidUseCase;
import com.nexus.auction.infrastructure.config.SecurityConfig;
import com.nexus.common.security.JwtAuthenticationFilter;
import com.nexus.common.security.JwtTokenProvider;
import com.nexus.common.security.PrivilegeAuthorizationAspect;
import com.nexus.common.web.GlobalExceptionHandler;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(BidController.class)
@ImportAutoConfiguration(AopAutoConfiguration.class)
@Import({GlobalExceptionHandler.class, AuctionApiMapperImpl.class, SecurityConfig.class,
        JwtAuthenticationFilter.class, PrivilegeAuthorizationAspect.class})
class BidControllerTest {

    @Autowired private MockMvc mockMvc;

    @MockBean private PlaceBidUseCase placeBidUseCase;
    @MockBean private GetBidHistoryUseCase getBidHistoryUseCase;
    @MockBean private JwtTokenProvider jwtTokenProvider;

    @Test
    void placeBid_returns401WithoutToken() throws Exception {
        mockMvc.perform(post("/api/v1/auctions/a-id/bids")
                        .contentType(APPLICATION_JSON)
                        .content("{\"amount\":100.00}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void placeBid_returns201AndUsesJwtSubjectAsBidder() throws Exception {
        when(jwtTokenProvider.isValid("good-token")).thenReturn(true);
        Claims claims = Jwts.claims().subject("bidder-id").add("privileges", List.of("AUCTION.BID")).build();
        when(jwtTokenProvider.parseClaims("good-token")).thenReturn(claims);
        when(placeBidUseCase.placeBid("a-id", "bidder-id", new BigDecimal("100.00")))
                .thenReturn(new BidResult("bid-id", "a-id", "bidder-id", new BigDecimal("100.00"), Instant.now()));

        mockMvc.perform(post("/api/v1/auctions/a-id/bids")
                        .header("Authorization", "Bearer good-token")
                        .contentType(APPLICATION_JSON)
                        .content("{\"amount\":100.00}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.data.bidderId").value("bidder-id"));
    }

    @Test
    void bidHistory_isPublicAndReturns200WithoutAToken() throws Exception {
        when(getBidHistoryUseCase.getHistory("a-id", 0, 20)).thenReturn(List.of());

        mockMvc.perform(get("/api/v1/auctions/a-id/bids"))
                .andExpect(status().isOk());
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cd /c/FPT/auction-service && mvn -Dtest=AuctionControllerTest,BidControllerTest test`
Expected: FAIL — `AuctionController`/`BidController` don't exist yet.

- [ ] **Step 5: Write `AuctionController`**

```java
package com.nexus.auction.api;

import com.nexus.auction.api.dto.request.CreateAuctionRequest;
import com.nexus.auction.api.dto.request.UpdateAuctionRequest;
import com.nexus.auction.api.dto.response.AuctionResponse;
import com.nexus.auction.api.mapper.AuctionApiMapper;
import com.nexus.auction.application.usecase.*;
import com.nexus.common.core.ApiResponse;
import com.nexus.common.security.RequiresPrivilege;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/auctions")
public class AuctionController {

    private final CreateAuctionUseCase createAuctionUseCase;
    private final UpdateAuctionUseCase updateAuctionUseCase;
    private final CancelAuctionUseCase cancelAuctionUseCase;
    private final AdminCancelAuctionUseCase adminCancelAuctionUseCase;
    private final GetAuctionUseCase getAuctionUseCase;
    private final ListAuctionsUseCase listAuctionsUseCase;
    private final AuctionApiMapper mapper;

    public AuctionController(CreateAuctionUseCase createAuctionUseCase, UpdateAuctionUseCase updateAuctionUseCase,
                              CancelAuctionUseCase cancelAuctionUseCase, AdminCancelAuctionUseCase adminCancelAuctionUseCase,
                              GetAuctionUseCase getAuctionUseCase, ListAuctionsUseCase listAuctionsUseCase,
                              AuctionApiMapper mapper) {
        this.createAuctionUseCase = createAuctionUseCase;
        this.updateAuctionUseCase = updateAuctionUseCase;
        this.cancelAuctionUseCase = cancelAuctionUseCase;
        this.adminCancelAuctionUseCase = adminCancelAuctionUseCase;
        this.getAuctionUseCase = getAuctionUseCase;
        this.listAuctionsUseCase = listAuctionsUseCase;
        this.mapper = mapper;
    }

    @RequiresPrivilege("AUCTION.CREATE")
    @PostMapping
    public ResponseEntity<ApiResponse<AuctionResponse>> create(Authentication authentication,
                                                                 @Valid @RequestBody CreateAuctionRequest request) {
        // sellerId MUST come from the JWT, never the request body — same reasoning as
        // catalog-service's CreateProductUseCase: a client must not create an auction under
        // another seller's identity.
        String sellerId = callerId(authentication);
        CreateAuctionCommand command = new CreateAuctionCommand(request.productId(), sellerId,
                request.startingPrice(), request.bidIncrement(), request.startTime(), request.endTime());
        AuctionResult result = createAuctionUseCase.create(command);
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.ok(mapper.toResponse(result)));
    }

    @RequiresPrivilege("AUCTION.UPDATE")
    @PutMapping("/{id}")
    public ResponseEntity<ApiResponse<AuctionResponse>> update(Authentication authentication,
                                                                 @PathVariable String id,
                                                                 @Valid @RequestBody UpdateAuctionRequest request) {
        AuctionResult result = updateAuctionUseCase.update(id, request.startingPrice(), request.bidIncrement(),
                request.startTime(), request.endTime(), callerId(authentication));
        return ResponseEntity.ok(ApiResponse.ok(mapper.toResponse(result)));
    }

    @RequiresPrivilege("AUCTION.CANCEL")
    @DeleteMapping("/{id}")
    public ResponseEntity<ApiResponse<AuctionResponse>> cancel(Authentication authentication, @PathVariable String id) {
        AuctionResult result = cancelAuctionUseCase.cancel(id, callerId(authentication));
        return ResponseEntity.ok(ApiResponse.ok(mapper.toResponse(result)));
    }

    @RequiresPrivilege("AUCTION.ADMIN_CANCEL")
    @PostMapping("/{id}/admin-cancel")
    public ResponseEntity<ApiResponse<AuctionResponse>> adminCancel(Authentication authentication, @PathVariable String id) {
        AuctionResult result = adminCancelAuctionUseCase.cancel(id, callerId(authentication));
        return ResponseEntity.ok(ApiResponse.ok(mapper.toResponse(result)));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<AuctionResponse>> get(@PathVariable String id) {
        AuctionResult result = getAuctionUseCase.get(id);
        return ResponseEntity.ok(ApiResponse.ok(mapper.toResponse(result)));
    }

    @GetMapping
    public ResponseEntity<ApiResponse<List<AuctionResponse>>> list(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String sellerId,
            @RequestParam(required = false) String productId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        List<AuctionResult> results = listAuctionsUseCase.list(new AuctionSearchQuery(status, sellerId, productId, page, size));
        return ResponseEntity.ok(ApiResponse.ok(results.stream().map(mapper::toResponse).toList()));
    }

    private static String callerId(Authentication authentication) {
        return (String) authentication.getPrincipal();
    }
}
```

- [ ] **Step 6: Write `BidController`**

```java
package com.nexus.auction.api;

import com.nexus.auction.api.dto.request.PlaceBidRequest;
import com.nexus.auction.api.dto.response.BidResponse;
import com.nexus.auction.api.mapper.AuctionApiMapper;
import com.nexus.auction.application.usecase.BidResult;
import com.nexus.auction.application.usecase.GetBidHistoryUseCase;
import com.nexus.auction.application.usecase.PlaceBidUseCase;
import com.nexus.common.core.ApiResponse;
import com.nexus.common.security.RequiresPrivilege;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/auctions/{auctionId}/bids")
public class BidController {

    private final PlaceBidUseCase placeBidUseCase;
    private final GetBidHistoryUseCase getBidHistoryUseCase;
    private final AuctionApiMapper mapper;

    public BidController(PlaceBidUseCase placeBidUseCase, GetBidHistoryUseCase getBidHistoryUseCase,
                          AuctionApiMapper mapper) {
        this.placeBidUseCase = placeBidUseCase;
        this.getBidHistoryUseCase = getBidHistoryUseCase;
        this.mapper = mapper;
    }

    @RequiresPrivilege("AUCTION.BID")
    @PostMapping
    public ResponseEntity<ApiResponse<BidResponse>> placeBid(Authentication authentication,
                                                                @PathVariable String auctionId,
                                                                @Valid @RequestBody PlaceBidRequest request) {
        // bidderId MUST come from the JWT — same reasoning as sellerId in AuctionController.
        String bidderId = (String) authentication.getPrincipal();
        BidResult result = placeBidUseCase.placeBid(auctionId, bidderId, request.amount());
        return ResponseEntity.status(HttpStatus.CREATED).body(ApiResponse.ok(mapper.toResponse(result)));
    }

    @GetMapping
    public ResponseEntity<ApiResponse<List<BidResponse>>> history(
            @PathVariable String auctionId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        List<BidResult> results = getBidHistoryUseCase.getHistory(auctionId, page, size);
        return ResponseEntity.ok(ApiResponse.ok(results.stream().map(mapper::toResponse).toList()));
    }
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd /c/FPT/auction-service && mvn -Dtest=AuctionControllerTest,BidControllerTest test`
Expected: PASS (9 tests total).

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/nexus/auction/api/
git add src/test/java/com/nexus/auction/api/AuctionControllerTest.java \
  src/test/java/com/nexus/auction/api/BidControllerTest.java
git commit -m "feat: add AuctionController and BidController"
```

---

### Task 16: `AuctionLifecycleJob` (`PENDING`→`ACTIVE`, `ACTIVE`→`ENDED` with settlement)

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/application/usecase/StartAuctionUseCase.java`, `auction-service/src/main/java/com/nexus/auction/application/usecase/EndAuctionUseCase.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/scheduling/AuctionLifecycleJob.java`
- Modify: `auction-service/src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java`
- Test: `auction-service/src/test/java/com/nexus/auction/infrastructure/scheduling/AuctionLifecycleJobTest.java`

**Interfaces:**
- Consumes: `AuctionRepositoryPort.findPendingReadyToStart`/`findActiveReadyToEnd`/`findByIdForUpdate` (Task 7), `EventPublisherPort` (Task 9), `com.nexus.common.events.AuctionStartedEvent`/`AuctionEndedEvent`/`AuctionWonEvent`/`AuctionFailedEvent` (Task 2).
- Produces: `StartAuctionUseCase.start(String auctionId)`, `EndAuctionUseCase.end(String auctionId)` — both idempotent (re-check status under the row lock before acting) and safely callable from a scheduled poller. `AuctionLifecycleJob` is the `@Scheduled` entry point; Task 17's `PaymentDeadlineJob` is a sibling in the same package.

Each use case here is called from `AuctionLifecycleJob`, a *different* Spring bean — this deliberately avoids the classic Spring self-invocation pitfall where an `@Transactional` **private method called from within the same class** silently runs with no transaction (the proxy that adds the transaction boundary is bypassed for in-class calls). Keeping `start`/`end` as their own beans, invoked from the job through their public method, means Spring's transaction proxy is actually in the call path.

- [ ] **Step 1: Write the failing test**

```java
package com.nexus.auction.infrastructure.scheduling;

import com.nexus.auction.application.usecase.EndAuctionUseCase;
import com.nexus.auction.application.usecase.StartAuctionUseCase;
import com.nexus.auction.infrastructure.persistence.OutboxJpaRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class AuctionLifecycleJobTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("auction_db").withUsername("nexus").withPassword("nexus");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eureka.client.enabled", () -> "false");
    }

    @Autowired private AuctionLifecycleJob job;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private OutboxJpaRepository outboxJpaRepository;

    private UUID seedAuction(String status, String startOffset, String endOffset,
                              String highestBidder, String highestBid) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO auctions (id, product_id, seller_id, starting_price, bid_increment,
                                       current_highest_bid, current_highest_bidder_id, status,
                                       start_time, end_time)
                VALUES (?, ?, 'seller-1', 100.00, 10.00, ?, ?, ?, now() + (? || ' minutes')::interval,
                        now() + (? || ' minutes')::interval)
                """, id, UUID.randomUUID(), highestBid, highestBidder, status, startOffset, endOffset);
        return id;
    }

    private long countByEventType(String eventType) {
        return outboxJpaRepository.findAll().stream()
                .filter(row -> row.getEventType().equals(eventType)).count();
    }

    @Test
    void run_transitionsPendingAuctionWhoseStartTimeHasPassedToActive() {
        UUID id = seedAuction("PENDING", "-10", "60", null, null);

        job.run();

        String status = jdbcTemplate.queryForObject("SELECT status FROM auctions WHERE id = ?", String.class, id);
        assertThat(status).isEqualTo("ACTIVE");
        assertThat(countByEventType("AuctionStarted")).isEqualTo(1);
    }

    @Test
    void run_endsActiveAuctionWithABidAndRecordsTheWinner() {
        UUID id = seedAuction("ACTIVE", "-60", "-1", "bidder-1", "150.00");

        job.run();

        String status = jdbcTemplate.queryForObject("SELECT status FROM auctions WHERE id = ?", String.class, id);
        String winnerId = jdbcTemplate.queryForObject("SELECT winner_id FROM auctions WHERE id = ?", String.class, id);
        assertThat(status).isEqualTo("ENDED");
        assertThat(winnerId).isEqualTo("bidder-1");
        assertThat(countByEventType("AuctionEnded")).isEqualTo(1);
        assertThat(countByEventType("AuctionWon")).isEqualTo(1);
    }

    @Test
    void run_endsActiveAuctionWithNoBidsAsFailed() {
        UUID id = seedAuction("ACTIVE", "-60", "-1", null, null);

        job.run();

        String status = jdbcTemplate.queryForObject("SELECT status FROM auctions WHERE id = ?", String.class, id);
        assertThat(status).isEqualTo("ENDED");
        assertThat(countByEventType("AuctionFailed")).isEqualTo(1);
    }

    @Test
    void run_isIdempotentAcrossOverlappingPolls() {
        seedAuction("ACTIVE", "-60", "-1", "bidder-1", "150.00");

        job.run();
        job.run();

        // The second run's findActiveReadyToEnd query returns nothing (the auction is already
        // ENDED), so no duplicate AuctionEnded/AuctionWon event is emitted.
        assertThat(countByEventType("AuctionEnded")).isEqualTo(1);
        assertThat(countByEventType("AuctionWon")).isEqualTo(1);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/FPT/auction-service && mvn -Dtest=AuctionLifecycleJobTest test`
Expected: FAIL — none of `StartAuctionUseCase`/`EndAuctionUseCase`/`AuctionLifecycleJob` exist yet.

- [ ] **Step 3: Write `StartAuctionUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.events.AuctionStartedEvent;
import org.springframework.transaction.annotation.Transactional;

public class StartAuctionUseCase {

    private final AuctionRepositoryPort auctionRepositoryPort;
    private final EventPublisherPort eventPublisherPort;

    public StartAuctionUseCase(AuctionRepositoryPort auctionRepositoryPort, EventPublisherPort eventPublisherPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
        this.eventPublisherPort = eventPublisherPort;
    }

    @Transactional
    public void start(String auctionId) {
        Auction auction = auctionRepositoryPort.findByIdForUpdate(auctionId).orElse(null);
        // Re-check under the lock: another poll tick (or this one, racing a manual admin action)
        // may have already moved this auction out of PENDING between the job's list query and
        // this per-row call — silently no-op rather than re-activating or erroring.
        if (auction == null || auction.getStatus() != AuctionStatus.PENDING) {
            return;
        }

        Auction started = auctionRepositoryPort.save(auction.withStatus(AuctionStatus.ACTIVE));
        eventPublisherPort.publish(new AuctionStartedEvent(started.getId()));
    }
}
```

- [ ] **Step 4: Write `EndAuctionUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.auction.domain.model.AuctionStatus;
import com.nexus.common.events.AuctionEndedEvent;
import com.nexus.common.events.AuctionFailedEvent;
import com.nexus.common.events.AuctionWonEvent;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class EndAuctionUseCase {

    private static final long PAYMENT_DEADLINE_HOURS = 24;

    private final AuctionRepositoryPort auctionRepositoryPort;
    private final EventPublisherPort eventPublisherPort;

    public EndAuctionUseCase(AuctionRepositoryPort auctionRepositoryPort, EventPublisherPort eventPublisherPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
        this.eventPublisherPort = eventPublisherPort;
    }

    @Transactional
    public void end(String auctionId) {
        Auction auction = auctionRepositoryPort.findByIdForUpdate(auctionId).orElse(null);
        // Same idempotency guard as StartAuctionUseCase: if this auction was already ended by an
        // earlier poll tick (or a prior call within this same tick), do nothing.
        if (auction == null || auction.getStatus() != AuctionStatus.ACTIVE) {
            return;
        }

        boolean hasWinner = auction.getCurrentHighestBidderId() != null;
        Auction ended;
        if (hasWinner) {
            Instant paymentDeadline = Instant.now().plus(PAYMENT_DEADLINE_HOURS, ChronoUnit.HOURS);
            ended = auction.withSettlement(auction.getCurrentHighestBidderId(), auction.getCurrentHighestBid(), paymentDeadline);
        } else {
            ended = auction.withStatus(AuctionStatus.ENDED);
        }
        Auction saved = auctionRepositoryPort.save(ended);

        eventPublisherPort.publish(new AuctionEndedEvent(saved.getId()));
        if (hasWinner) {
            eventPublisherPort.publish(new AuctionWonEvent(saved.getId(), saved.getProductId(), saved.getSellerId(),
                    saved.getWinnerId(), saved.getFinalPrice()));
        } else {
            eventPublisherPort.publish(new AuctionFailedEvent(saved.getId()));
        }
    }
}
```

- [ ] **Step 5: Write `AuctionLifecycleJob`**

```java
package com.nexus.auction.infrastructure.scheduling;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.usecase.EndAuctionUseCase;
import com.nexus.auction.application.usecase.StartAuctionUseCase;
import com.nexus.auction.domain.model.Auction;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class AuctionLifecycleJob {

    private final AuctionRepositoryPort auctionRepositoryPort;
    private final StartAuctionUseCase startAuctionUseCase;
    private final EndAuctionUseCase endAuctionUseCase;

    public AuctionLifecycleJob(AuctionRepositoryPort auctionRepositoryPort, StartAuctionUseCase startAuctionUseCase,
                                EndAuctionUseCase endAuctionUseCase) {
        this.auctionRepositoryPort = auctionRepositoryPort;
        this.startAuctionUseCase = startAuctionUseCase;
        this.endAuctionUseCase = endAuctionUseCase;
    }

    @Scheduled(fixedDelay = 10000)
    public void run() {
        Instant now = Instant.now();
        for (Auction auction : auctionRepositoryPort.findPendingReadyToStart(now)) {
            startAuctionUseCase.start(auction.getId());
        }
        for (Auction auction : auctionRepositoryPort.findActiveReadyToEnd(now)) {
            endAuctionUseCase.end(auction.getId());
        }
    }
}
```

- [ ] **Step 6: Add both use case beans to `UseCaseConfig`**

```java
    @Bean
    public StartAuctionUseCase startAuctionUseCase(AuctionRepositoryPort auctionPort,
                                                     EventPublisherPort eventPublisherPort) {
        return new StartAuctionUseCase(auctionPort, eventPublisherPort);
    }

    @Bean
    public EndAuctionUseCase endAuctionUseCase(AuctionRepositoryPort auctionPort,
                                                EventPublisherPort eventPublisherPort) {
        return new EndAuctionUseCase(auctionPort, eventPublisherPort);
    }
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=AuctionLifecycleJobTest test`
Expected: PASS (4 tests).

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/nexus/auction/application/usecase/StartAuctionUseCase.java \
  src/main/java/com/nexus/auction/application/usecase/EndAuctionUseCase.java \
  src/main/java/com/nexus/auction/infrastructure/scheduling/AuctionLifecycleJob.java \
  src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java \
  src/test/java/com/nexus/auction/infrastructure/scheduling/AuctionLifecycleJobTest.java
git commit -m "feat: add AuctionLifecycleJob with idempotent start/end use cases"
```

---

### Task 17: `PaymentDeadlineJob`

**Files:**
- Create: `auction-service/src/main/java/com/nexus/auction/application/usecase/EmitPaymentTimeoutUseCase.java`, `auction-service/src/main/java/com/nexus/auction/infrastructure/scheduling/PaymentDeadlineJob.java`
- Modify: `auction-service/src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java`
- Test: `auction-service/src/test/java/com/nexus/auction/infrastructure/scheduling/PaymentDeadlineJobTest.java`

**Interfaces:**
- Consumes: `AuctionRepositoryPort.findEndedAwaitingPaymentTimeout` (Task 7), `EventPublisherPort` (Task 9), `com.nexus.common.events.AuctionPaymentTimeoutEvent` (Task 2).
- Produces: `EmitPaymentTimeoutUseCase.emit(String auctionId)` — idempotent via the `payment_timeout_emitted` flag (this is the concrete mechanism promised by the spec's dependency-deferral decision: the flag guarantees "fires once" even though nothing in this codebase can ever clear it once a real payment arrives, since Commerce doesn't exist).

- [ ] **Step 1: Write the failing test**

```java
package com.nexus.auction.infrastructure.scheduling;

import com.nexus.auction.infrastructure.persistence.OutboxJpaRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class PaymentDeadlineJobTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("auction_db").withUsername("nexus").withPassword("nexus");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eureka.client.enabled", () -> "false");
    }

    @Autowired private PaymentDeadlineJob job;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private OutboxJpaRepository outboxJpaRepository;

    private UUID seedEndedAuctionPastDeadline() {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO auctions (id, product_id, seller_id, starting_price, bid_increment,
                                       current_highest_bid, current_highest_bidder_id, status,
                                       start_time, end_time, winner_id, final_price, payment_deadline,
                                       payment_timeout_emitted)
                VALUES (?, ?, 'seller-1', 100.00, 10.00, 150.00, 'bidder-1', 'ENDED',
                        now() - interval '2 hours', now() - interval '1 hour', 'bidder-1', 150.00,
                        now() - interval '1 minute', false)
                """, id, UUID.randomUUID());
        return id;
    }

    private long paymentTimeoutEventCount() {
        return outboxJpaRepository.findAll().stream()
                .filter(row -> row.getEventType().equals("AuctionPaymentTimeout")).count();
    }

    @Test
    void run_emitsPaymentTimeoutForAWinnerPastTheDeadline() {
        UUID id = seedEndedAuctionPastDeadline();

        job.run();

        Boolean emitted = jdbcTemplate.queryForObject(
                "SELECT payment_timeout_emitted FROM auctions WHERE id = ?", Boolean.class, id);
        assertThat(emitted).isTrue();
        assertThat(paymentTimeoutEventCount()).isEqualTo(1);
    }

    @Test
    void run_doesNotDoubleEmitOnOverlappingPolls() {
        seedEndedAuctionPastDeadline();

        job.run();
        job.run();

        assertThat(paymentTimeoutEventCount()).isEqualTo(1);
    }

    @Test
    void run_doesNotEmitForAnAuctionStillWithinTheDeadline() {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO auctions (id, product_id, seller_id, starting_price, bid_increment,
                                       current_highest_bid, current_highest_bidder_id, status,
                                       start_time, end_time, winner_id, final_price, payment_deadline,
                                       payment_timeout_emitted)
                VALUES (?, ?, 'seller-1', 100.00, 10.00, 150.00, 'bidder-1', 'ENDED',
                        now() - interval '2 hours', now() - interval '1 hour', 'bidder-1', 150.00,
                        now() + interval '23 hours', false)
                """, id, UUID.randomUUID());

        job.run();

        assertThat(paymentTimeoutEventCount()).isZero();
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /c/FPT/auction-service && mvn -Dtest=PaymentDeadlineJobTest test`
Expected: FAIL — `EmitPaymentTimeoutUseCase`/`PaymentDeadlineJob` don't exist yet.

- [ ] **Step 3: Write `EmitPaymentTimeoutUseCase`**

```java
package com.nexus.auction.application.usecase;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.port.out.EventPublisherPort;
import com.nexus.auction.domain.model.Auction;
import com.nexus.common.events.AuctionPaymentTimeoutEvent;
import org.springframework.transaction.annotation.Transactional;

public class EmitPaymentTimeoutUseCase {

    private final AuctionRepositoryPort auctionRepositoryPort;
    private final EventPublisherPort eventPublisherPort;

    public EmitPaymentTimeoutUseCase(AuctionRepositoryPort auctionRepositoryPort, EventPublisherPort eventPublisherPort) {
        this.auctionRepositoryPort = auctionRepositoryPort;
        this.eventPublisherPort = eventPublisherPort;
    }

    @Transactional
    public void emit(String auctionId) {
        Auction auction = auctionRepositoryPort.findByIdForUpdate(auctionId).orElse(null);
        // The payment_timeout_emitted flag (checked again here, under the lock, not just in the
        // job's list query) is what makes this safe to call twice for the same auction across
        // overlapping poll ticks: once true, this is a no-op forever.
        if (auction == null || auction.isPaymentTimeoutEmitted() || auction.getWinnerId() == null) {
            return;
        }

        Auction flagged = auctionRepositoryPort.save(auction.withPaymentTimeoutEmitted());
        eventPublisherPort.publish(new AuctionPaymentTimeoutEvent(flagged.getId(), flagged.getWinnerId()));
    }
}
```

- [ ] **Step 4: Write `PaymentDeadlineJob`**

```java
package com.nexus.auction.infrastructure.scheduling;

import com.nexus.auction.application.port.out.AuctionRepositoryPort;
import com.nexus.auction.application.usecase.EmitPaymentTimeoutUseCase;
import com.nexus.auction.domain.model.Auction;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class PaymentDeadlineJob {

    private final AuctionRepositoryPort auctionRepositoryPort;
    private final EmitPaymentTimeoutUseCase emitPaymentTimeoutUseCase;

    public PaymentDeadlineJob(AuctionRepositoryPort auctionRepositoryPort,
                               EmitPaymentTimeoutUseCase emitPaymentTimeoutUseCase) {
        this.auctionRepositoryPort = auctionRepositoryPort;
        this.emitPaymentTimeoutUseCase = emitPaymentTimeoutUseCase;
    }

    @Scheduled(fixedDelay = 30000)
    public void run() {
        Instant now = Instant.now();
        for (Auction auction : auctionRepositoryPort.findEndedAwaitingPaymentTimeout(now)) {
            emitPaymentTimeoutUseCase.emit(auction.getId());
        }
    }
}
```

- [ ] **Step 5: Add the bean to `UseCaseConfig`**

```java
    @Bean
    public EmitPaymentTimeoutUseCase emitPaymentTimeoutUseCase(AuctionRepositoryPort auctionPort,
                                                                 EventPublisherPort eventPublisherPort) {
        return new EmitPaymentTimeoutUseCase(auctionPort, eventPublisherPort);
    }
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd /c/FPT/auction-service && mvn -Dtest=PaymentDeadlineJobTest test`
Expected: PASS (3 tests).

- [ ] **Step 7: Run the full test suite before moving to cross-repo work**

Run: `cd /c/FPT/auction-service && mvn -B clean verify`
Expected: `BUILD SUCCESS`, all tests from Tasks 1–17 green.

- [ ] **Step 8: Commit and push**

```bash
git add src/main/java/com/nexus/auction/application/usecase/EmitPaymentTimeoutUseCase.java \
  src/main/java/com/nexus/auction/infrastructure/scheduling/PaymentDeadlineJob.java \
  src/main/java/com/nexus/auction/infrastructure/config/UseCaseConfig.java \
  src/test/java/com/nexus/auction/infrastructure/scheduling/PaymentDeadlineJobTest.java
git commit -m "feat: add PaymentDeadlineJob with idempotent timeout emission"
git push origin main
```

---

### Task 18: `user-service` — seed `AUCTION.*` privileges

**Note on a spec correction made during planning:** the design spec said new privileges would be "added to `common-libs`'s `common-security` module." That is not how privileges actually work in this codebase — `@RequiresPrivilege` values are free-form strings checked against the JWT's `privileges` claim at request time; the actual privilege catalog and role assignments live in **`user-service`'s own Postgres database**, seeded via its Flyway migrations (see `user-service/src/main/resources/db/migration/V4__seed_catalog_privileges.sql` and `V5__seed_admin_only_privileges.sql`, which did exactly this for `catalog-service`'s `PRODUCT.*`/`CATEGORY.*` privileges). Without this task, **no role — not even ADMIN — could call any `AUCTION.*`-gated endpoint**, the same gap `V4`'s own comment describes happening to Catalog. `common-libs` has no involvement in privilege seeding at all.

**Files:**
- Create (in `C:\FPT\user-service`): `src/main/resources/db/migration/V6__seed_auction_privileges.sql`

**Interfaces:**
- Produces: 5 new rows in `privileges`, assigned to `roles` via `role_privileges` — required before Task 15's `@RequiresPrivilege`-gated endpoints are callable by anyone, including in the manual smoke test in Task 20.

- [ ] **Step 1: Write the migration**

```sql
-- V6__seed_auction_privileges.sql
-- auction-service's AuctionController/BidController enforce these privilege codes via
-- @RequiresPrivilege (AUCTION.VIEW/LIST/VIEW_BID_HISTORY exist in the SRS's privilege table but
-- are not gated anywhere — their endpoints are public, same treatment as PRODUCT.VIEW/LIST/SEARCH
-- in V4 — so, matching that precedent, they are deliberately NOT seeded here either).
INSERT INTO privileges (code) VALUES
    ('AUCTION.CREATE'), ('AUCTION.UPDATE'), ('AUCTION.CANCEL'), ('AUCTION.ADMIN_CANCEL'), ('AUCTION.BID');

-- ADMIN already gets "every seeded privilege" per V2, but that INSERT ran at V2 time and does
-- not retroactively cover privileges added later (same caveat V4 and V5 both had to repeat).
INSERT INTO role_privileges (role_id, privilege_id)
SELECT (SELECT id FROM roles WHERE code = 'ADMIN'), id
FROM privileges
WHERE code IN ('AUCTION.CREATE', 'AUCTION.UPDATE', 'AUCTION.CANCEL', 'AUCTION.ADMIN_CANCEL', 'AUCTION.BID');

-- SELLER creates and manages its own auctions, and may also bid on other sellers' auctions.
INSERT INTO role_privileges (role_id, privilege_id)
SELECT (SELECT id FROM roles WHERE code = 'SELLER'), id
FROM privileges
WHERE code IN ('AUCTION.CREATE', 'AUCTION.UPDATE', 'AUCTION.CANCEL', 'AUCTION.BID');

-- BUYER only ever bids; it cannot create, edit, or cancel an auction.
INSERT INTO role_privileges (role_id, privilege_id)
SELECT (SELECT id FROM roles WHERE code = 'BUYER'), id
FROM privileges
WHERE code IN ('AUCTION.BID');

-- AUCTION.ADMIN_CANCEL is intentionally ADMIN-only (not given to SELLER), mirroring
-- PRODUCT.MANAGE_ANY in V5: it is the signal that distinguishes an admin's override power from
-- an ordinary seller's own-auction-only power.
```

- [ ] **Step 2: Run `user-service`'s tests to verify the migration applies cleanly**

Run: `cd /c/FPT/user-service && mvn -B clean verify`
Expected: `BUILD SUCCESS` — Flyway applies `V6` alongside the existing migrations with no checksum conflicts (Flyway migrations are immutable once applied in a real environment, but this is a fresh `V6` file, not an edit to `V1`–`V5`, so there is nothing to migrate-repair).

- [ ] **Step 3: Commit and push**

```bash
cd /c/FPT/user-service
git add src/main/resources/db/migration/V6__seed_auction_privileges.sql
git commit -m "feat: seed AUCTION.* privileges for ADMIN, SELLER, BUYER"
git push origin main
```

---

### Task 19: `api-gateway` — route and public-path filter for `/api/v1/auctions/**`

**Files:**
- Modify (in `C:\FPT\api-gateway`): `src/main/resources/application.yml`, `src/main/java/com/nexus/gateway/filter/JwtValidationGlobalFilter.java`
- Test: `src/test/java/com/nexus/gateway/filter/JwtValidationGlobalFilterTest.java` (append 2 tests)

**Interfaces:**
- Consumes: nothing new — this task only extends the existing `catalog-service` route/filter pattern to a third path prefix.
- Produces: `/api/v1/auctions/**` routed to `auction-service` via Eureka; `GET` requests under it public, all other methods requiring a valid JWT (checked coarsely here; privilege enforcement itself happens inside `auction-service`, per Task 15).

- [ ] **Step 1: Add the route**

In `application.yml`, under `spring.cloud.gateway.routes`, add:

```yaml
        - id: auction-service
          uri: lb://auction-service
          predicates:
            - Path=/api/v1/auctions/**
```

- [ ] **Step 2: Write the two failing filter tests**

Append to `JwtValidationGlobalFilterTest`:

```java
    @Test
    void allowsGetOnAuctionsPathWithoutToken() {
        when(chain.filter(any())).thenReturn(Mono.empty());
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/auctions/a-id/bids").build());

        filter.filter(exchange, chain).block();

        verify(chain).filter(exchange);
    }

    @Test
    void rejectsPostOnAuctionsBidsPathWithoutToken() {
        ServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/v1/auctions/a-id/bids").build());

        filter.filter(exchange, chain).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        verify(chain, never()).filter(any());
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd /c/FPT/api-gateway && mvn -Dtest=JwtValidationGlobalFilterTest test`
Expected: FAIL — `/api/v1/auctions/**` isn't recognized as public yet, so `allowsGetOnAuctionsPathWithoutToken` gets a 401 instead of passing through.

- [ ] **Step 4: Add the `AUCTIONS_PATTERN` and extend `isPublic`**

In `JwtValidationGlobalFilter.java`, add alongside the existing two pattern constants:

```java
    private static final String AUCTIONS_PATTERN = "/api/v1/auctions/**";
```

Change the `isPublic` GET check from:

```java
        if (HttpMethod.GET.equals(request.getMethod())
                && (PATH_MATCHER.match(PRODUCTS_PATTERN, path) || PATH_MATCHER.match(CATEGORIES_PATTERN, path))) {
            return true;
        }
```

to:

```java
        if (HttpMethod.GET.equals(request.getMethod())
                && (PATH_MATCHER.match(PRODUCTS_PATTERN, path) || PATH_MATCHER.match(CATEGORIES_PATTERN, path)
                    || PATH_MATCHER.match(AUCTIONS_PATTERN, path))) {
            return true;
        }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd /c/FPT/api-gateway && mvn -B clean verify`
Expected: `BUILD SUCCESS`, all `JwtValidationGlobalFilterTest` cases (existing + 2 new) green.

- [ ] **Step 6: Commit and push**

```bash
cd /c/FPT/api-gateway
git add src/main/resources/application.yml src/main/java/com/nexus/gateway/filter/JwtValidationGlobalFilter.java \
  src/test/java/com/nexus/gateway/filter/JwtValidationGlobalFilterTest.java
git commit -m "feat: route /api/v1/auctions/** to auction-service"
git push origin main
```

---

### Task 20: `infra` — wire `auction-service` into docker-compose and `build-all.sh`

**Files:**
- Modify (in `C:\FPT\infra`): `docker-compose.yml`, `build-all.sh`, `README.md`

**Interfaces:**
- Consumes: `auction-service`'s `Dockerfile` (Task 1) and the pre-built jar `build-all.sh` produces.
- Produces: `docker compose up` now also starts `postgres-auction` and `auction-service`, completing the spec's Goal ("through the gateway... automatically transition through its lifecycle") as an actually-runnable cluster.

- [ ] **Step 1: Add `postgres-auction` and `auction-service` to `docker-compose.yml`**

Add a new Postgres service (following the exact shape of `postgres-catalog`, next port after `5433`):

```yaml
  postgres-auction:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: auction_db
      POSTGRES_USER: nexus
      POSTGRES_PASSWORD: nexus
    ports:
      - "5434:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U nexus -d auction_db"]
      interval: 5s
      timeout: 5s
      retries: 10
```

Add the service block (following `catalog-service`'s exact shape, port `8083`):

```yaml
  auction-service:
    build:
      context: ../auction-service
    depends_on:
      postgres-auction:
        condition: service_healthy
      discovery-server:
        condition: service_started
      kafka:
        condition: service_started
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres-auction:5432/auction_db
      EUREKA_CLIENT_SERVICEURL_DEFAULTZONE: http://discovery-server:8761/eureka
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    ports:
      - "8083:8083"
```

- [ ] **Step 2: Add `auction-service` to `build-all.sh`'s repo list and build loop**

Change:
```bash
for repo in common-libs discovery-server api-gateway user-service catalog-service; do
```
to:
```bash
for repo in common-libs discovery-server api-gateway user-service catalog-service auction-service; do
```
(this line appears twice in the file — the existence-check loop and the `mvn clean package` loop; update both).

- [ ] **Step 3: Update `README.md`'s expected layout and port list**

In the "Expected layout" code block, add `auction-service/` alongside `catalog-service/`. In "Once up:", add: `` `auction-service` directly at `:8083` ``.

- [ ] **Step 4: Validate the compose file and run the full cluster**

Run: `cd /c/FPT/infra && docker compose config --quiet`
Expected: no output, exit code 0 (valid YAML, all `${...}` references resolve).

Run: `cd /c/FPT/infra && ./build-all.sh && docker compose build && docker compose up -d`
Expected: all 8 containers (`postgres-user`, `postgres-catalog`, `postgres-auction`, `kafka`, `discovery-server`, `api-gateway`, `user-service`, `catalog-service`, `auction-service`) start; `auction-service` registers with Eureka (visible at `http://localhost:8761`).

Manual smoke check (no scripted smoke test exists for this cluster yet — same gap `infra/README.md`'s own "Follow-up" section already notes for the whole cluster, not something this task should silently take on): log in via `user-service` as a seeded SELLER, `POST /api/v1/auctions` through the gateway with a real `productId` from `catalog-service`, confirm `201` and that the row appears via `GET /api/v1/auctions/{id}`.

Run: `docker compose down`

- [ ] **Step 5: Commit and push**

```bash
cd /c/FPT/infra
git add docker-compose.yml build-all.sh README.md
git commit -m "feat: wire auction-service into the docker-compose cluster"
git push origin main
```
