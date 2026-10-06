# Notification Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `notification-service` repo that consumes the three existing Kafka topics (`user-events`, `catalog-events`, `auction-events`), durably records every event for traceability, derives a human-readable per-user notification for five known event types, and exposes a personal inbox API plus an audit API — then wires it into the rest of the running system (gateway route, privilege seed, docker-compose).

**Architecture:** Same hexagonal layering as `user-service`/`catalog-service`/`auction-service` (api/application/domain/infrastructure), but the inbound adapter is three `@KafkaListener` methods instead of only REST controllers. A single `notifications` table (one nullable `recipient_user_id`/`message` pair) serves both the personal inbox and the audit view, deduplicated via a unique `event_id` column.

**Tech Stack:** Java 21, Spring Boot 3.3.4, Spring Cloud 2023.0.3 (Eureka client only, no gateway), Spring Data JPA + Postgres 16 + Flyway, Spring Kafka, Spring Security (stateless JWT via `common-security`), MapStruct 1.6.2, JUnit 5 + AssertJ + Mockito + Testcontainers 1.20.1 (Postgres, Kafka). Depends on `com.nexus:common-core`, `common-web`, `common-security`, `common-events` at version `1.1.0` (already published — no `common-libs` version bump needed; all five mapped event classes already exist at this version).

**Spec:** `docs/superpowers/specs/2026-10-06-notification-service-design.md`

## Global Constraints

- One Postgres DB per service: new `notification_db`, port `5435`, Flyway-migrated from `src/main/resources/db/migration/V1__*.sql`.
- Port `8084` (next available per the project's port convention — see `CLAUDE.md`).
- `event_id` (present on every `DomainEvent` subclass) is the idempotency key — unique DB constraint, not an application-level check-then-insert (races under Kafka redelivery must be caught by the database, not assumed away).
- **Deviation from the project's usual SecurityConfig convention, stated explicitly because it is easy to copy-paste wrong:** every other service does `requestMatchers(HttpMethod.GET, "/api/v1/**").permitAll()` because its GET data (products, categories, public auction listings) is genuinely public. Nothing in this service is public — `/me` is personal, `/` is privileged audit data, `/read` mutates a specific user's own row — so `SecurityConfig` here has **no blanket GET-permitAll matcher**; only `/actuator/**` is open, everything under `/api/v1/**` requires `anyRequest().authenticated()`.
- Five event types get a derived recipient/message (`UserRegistered`, `BidPlaced`, `Outbid`, `AuctionWon`, `AuctionSettled` — these are the `eventType` string values, not the Java class names, though they're close); the other twelve are still recorded in full with `recipient_user_id`/`message` left `NULL`. Do not add more mappings — out of scope per the spec.
- No email/SMS/push, no DLQ, no WebSocket push — reading the spec's "Out of scope" section before adding anything not listed in a task below will save a revert.
- `NotificationMessageResolver` depends on `common-events` concrete classes, so — consistent with how `application/usecase` classes in every other service already import `common-events` — it lives in `application`, not `domain/service`. (`domain/service` in this codebase never imports anything outside `domain/model` and the JDK; confirmed by reading `auction-service`'s `AntiSnipingPolicy`. The design spec said "domain/service" loosely; this plan corrects that to match the actual established convention.)

## Review Focus

- **Kafka redelivers the same event (at-least-once guarantee)** → must result in exactly one row, not a crashed consumer or a duplicate. Pinned in Task 2 (`NotificationRepositoryAdapterTest`) and re-verified end-to-end in Task 5 (`AuctionEventsListenerIntegrationTest`).
- **A message on the topic is not valid JSON, or is missing an expected field** → the listener must log and move on, not throw out of the Kafka consumer thread (which would stop that partition's consumption entirely). Pinned in Task 5.
- **An event type outside the five mapped ones arrives** (eleven of the seventeen total types) → must still be recorded with `recipient_user_id`/`message` as `NULL`, never silently dropped. Pinned in Task 4.
- **A caller tries to mark another user's notification as read** → must be `403`, never a silent success that lets one user manipulate another's inbox state. Pinned in Task 7.
- **A caller without `NOTIFICATION.AUDIT` hits `GET /api/v1/notifications`** → must be `403` with the full dataset never serialized into the response at all. Pinned in Task 7.

---

### Task 1: Scaffold the `notification-service` repo

**Files:**
- Create: `notification-service/pom.xml`
- Create: `notification-service/src/main/java/com/nexus/notification/NotificationServiceApplication.java`
- Create: `notification-service/src/main/resources/application.yml`
- Create: `notification-service/Dockerfile`
- Create: `notification-service/.gitignore`

**Interfaces:**
- Produces: a compiling, empty Spring Boot project other tasks add files into. No runtime behavior yet.

- [ ] **Step 1: Create the repo directory and initialize git**

```bash
mkdir -p /c/FPT/notification-service
cd /c/FPT/notification-service
git init
```

- [ ] **Step 2: Write `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.nexus</groupId>
  <artifactId>notification-service</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <properties>
    <java.version>21</java.version>
    <maven.compiler.source>21</maven.compiler.source>
    <maven.compiler.target>21</maven.compiler.target>
    <maven.compiler.parameters>true</maven.compiler.parameters>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <spring-boot.version>3.3.4</spring-boot.version>
    <spring-cloud.version>2023.0.3</spring-cloud.version>
    <mapstruct.version>1.6.2</mapstruct.version>
    <testcontainers.version>1.20.1</testcontainers.version>
    <!-- Already published; has all 5 event classes this service maps. No bump needed. -->
    <common-libs.version>1.1.0</common-libs.version>
  </properties>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-dependencies</artifactId>
        <version>${spring-boot.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
      <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-dependencies</artifactId>
        <version>${spring-cloud.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>testcontainers-bom</artifactId>
        <version>${testcontainers.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <dependencies>
    <dependency>
      <groupId>com.nexus</groupId>
      <artifactId>common-core</artifactId>
      <version>${common-libs.version}</version>
    </dependency>
    <dependency>
      <groupId>com.nexus</groupId>
      <artifactId>common-web</artifactId>
      <version>${common-libs.version}</version>
    </dependency>
    <dependency>
      <groupId>com.nexus</groupId>
      <artifactId>common-events</artifactId>
      <version>${common-libs.version}</version>
    </dependency>
    <dependency>
      <groupId>com.nexus</groupId>
      <artifactId>common-security</artifactId>
      <version>${common-libs.version}</version>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-security</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.cloud</groupId>
      <artifactId>spring-cloud-starter-netflix-eureka-client</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.kafka</groupId>
      <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>org.flywaydb</groupId>
      <artifactId>flyway-core</artifactId>
    </dependency>
    <dependency>
      <groupId>org.flywaydb</groupId>
      <artifactId>flyway-database-postgresql</artifactId>
    </dependency>
    <dependency>
      <groupId>org.mapstruct</groupId>
      <artifactId>mapstruct</artifactId>
      <version>${mapstruct.version}</version>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>junit-jupiter</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>postgresql</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>kafka</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
        <version>${spring-boot.version}</version>
        <executions>
          <execution>
            <goals>
              <goal>repackage</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <configuration>
          <annotationProcessorPaths>
            <path>
              <groupId>org.mapstruct</groupId>
              <artifactId>mapstruct-processor</artifactId>
              <version>${mapstruct.version}</version>
            </path>
          </annotationProcessorPaths>
        </configuration>
      </plugin>
    </plugins>
  </build>

  <repositories>
    <repository>
      <id>github</id>
      <name>GitHub Packages - common-libs</name>
      <url>https://maven.pkg.github.com/antran19/common-libs</url>
    </repository>
  </repositories>
</project>
```

- [ ] **Step 3: Write the main application class**

```java
package com.nexus.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication(scanBasePackages = "com.nexus")
@EnableDiscoveryClient
public class NotificationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
```

`scanBasePackages = "com.nexus"` (not the default `com.nexus.notification`) is required so Spring picks up `common-security`'s `JwtAuthenticationFilter`/`PrivilegeAuthorizationAspect` beans, which live under `com.nexus.common.security` — every other service in this codebase does the same.

- [ ] **Step 4: Write `application.yml`**

```yaml
server:
  port: 8084

spring:
  application:
    name: notification-service
  datasource:
    url: jdbc:postgresql://localhost:5435/notification_db
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
    consumer:
      # Shared across all 3 listener methods. If this service is ever scaled to multiple
      # instances, Kafka's consumer-group partition assignment (not this service's own
      # code) is what prevents the same message being processed twice.
      group-id: notification-service
      # Only affects a brand-new consumer group with no committed offset yet (i.e. the
      # very first time this service ever runs against a given topic) -- picks up events
      # that were already on the topic before this service existed, which matches the
      # "record everything for traceability" goal. After that first run, the group's
      # committed offsets govern as normal regardless of this setting.
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

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

- [ ] **Step 5: Write the `Dockerfile`**

```dockerfile
FROM eclipse-temurin:21-jre
COPY target/notification-service-0.1.0-SNAPSHOT.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

- [ ] **Step 6: Write `.gitignore`**

```
target/
*.class
.idea/
*.iml
```

- [ ] **Step 7: Verify it compiles**

Run: `cd /c/FPT/notification-service && mvn -q compile`
Expected: no output, exit code 0. (If it fails on resolving `com.nexus:common-*`, run `cd ../common-libs && mvn -q clean install -DskipTests` first — same precondition every other service has.)

- [ ] **Step 8: Create the GitHub repo and push**

```bash
cd /c/FPT/notification-service
git add pom.xml src/main/java/com/nexus/notification/NotificationServiceApplication.java \
  src/main/resources/application.yml Dockerfile .gitignore
git commit -m "chore: scaffold notification-service Spring Boot project"
gh repo create antran19/notification-service --public --source=. --remote=origin
git push -u origin main
```

---

### Task 2: Persistence layer — `notifications` table, domain model, repository adapter

**Files:**
- Create: `notification-service/src/main/resources/db/migration/V1__create_notifications_table.sql`
- Create: `notification-service/src/main/java/com/nexus/notification/domain/model/Notification.java`
- Create: `notification-service/src/main/java/com/nexus/notification/application/port/out/NotificationRepositoryPort.java`
- Create: `notification-service/src/main/java/com/nexus/notification/infrastructure/persistence/entity/NotificationJpaEntity.java`
- Create: `notification-service/src/main/java/com/nexus/notification/infrastructure/persistence/NotificationJpaRepository.java`
- Create: `notification-service/src/main/java/com/nexus/notification/infrastructure/persistence/NotificationRepositoryAdapter.java`
- Create: `notification-service/src/main/java/com/nexus/notification/infrastructure/config/UseCaseConfig.java` (empty `@Configuration` shell for now, later tasks add `@Bean` methods)
- Test: `notification-service/src/test/java/com/nexus/notification/infrastructure/persistence/NotificationRepositoryAdapterTest.java`

**Interfaces:**
- Produces: `Notification` (domain model) with fields `id, eventId, eventType, aggregateId, recipientUserId, message, payload, read, occurredAt, createdAt`, static factory `Notification.record(String eventId, String eventType, String aggregateId, String recipientUserId, String message, String payload, Instant occurredAt)`, instance method `markRead()`, and getters for every field (`isRead()` for the boolean).
- Produces: `NotificationRepositoryPort` with `void record(Notification notification)`, `void update(Notification notification)`, `List<Notification> findByRecipientUserId(String recipientUserId)`, `List<Notification> findAllFiltered(String eventType, String aggregateId)`, `Optional<Notification> findById(String id)`.
- Consumes: nothing from earlier tasks.

- [ ] **Step 1: Write the Flyway migration**

```sql
-- V1__create_notifications_table.sql
CREATE TABLE notifications (
    id UUID PRIMARY KEY,
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

- [ ] **Step 2: Write the domain model**

```java
package com.nexus.notification.domain.model;

import java.time.Instant;
import java.util.UUID;

public class Notification {

    private final String id;
    private final String eventId;
    private final String eventType;
    private final String aggregateId;
    private final String recipientUserId;
    private final String message;
    private final String payload;
    private boolean read;
    private final Instant occurredAt;
    private final Instant createdAt;

    public Notification(String id, String eventId, String eventType, String aggregateId,
                         String recipientUserId, String message, String payload,
                         boolean read, Instant occurredAt, Instant createdAt) {
        this.id = id;
        this.eventId = eventId;
        this.eventType = eventType;
        this.aggregateId = aggregateId;
        this.recipientUserId = recipientUserId;
        this.message = message;
        this.payload = payload;
        this.read = read;
        this.occurredAt = occurredAt;
        this.createdAt = createdAt;
    }

    public static Notification record(String eventId, String eventType, String aggregateId,
                                       String recipientUserId, String message, String payload,
                                       Instant occurredAt) {
        return new Notification(UUID.randomUUID().toString(), eventId, eventType, aggregateId,
                recipientUserId, message, payload, false, occurredAt, Instant.now());
    }

    public void markRead() {
        this.read = true;
    }

    public String getId() { return id; }
    public String getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public String getAggregateId() { return aggregateId; }
    public String getRecipientUserId() { return recipientUserId; }
    public String getMessage() { return message; }
    public String getPayload() { return payload; }
    public boolean isRead() { return read; }
    public Instant getOccurredAt() { return occurredAt; }
    public Instant getCreatedAt() { return createdAt; }
}
```

- [ ] **Step 3: Write the port**

```java
package com.nexus.notification.application.port.out;

import com.nexus.notification.domain.model.Notification;

import java.util.List;
import java.util.Optional;

public interface NotificationRepositoryPort {
    void record(Notification notification);
    void update(Notification notification);
    List<Notification> findByRecipientUserId(String recipientUserId);
    List<Notification> findAllFiltered(String eventType, String aggregateId);
    Optional<Notification> findById(String id);
}
```

- [ ] **Step 4: Write the JPA entity**

```java
package com.nexus.notification.infrastructure.persistence.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notifications")
public class NotificationJpaEntity {

    @Id
    private UUID id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "aggregate_id")
    private String aggregateId;

    @Column(name = "recipient_user_id")
    private String recipientUserId;

    @Column(columnDefinition = "TEXT")
    private String message;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "is_read", nullable = false)
    private boolean read;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected NotificationJpaEntity() {
    }

    public NotificationJpaEntity(UUID id, String eventId, String eventType, String aggregateId,
                                  String recipientUserId, String message, String payload,
                                  boolean read, Instant occurredAt, Instant createdAt) {
        this.id = id;
        this.eventId = eventId;
        this.eventType = eventType;
        this.aggregateId = aggregateId;
        this.recipientUserId = recipientUserId;
        this.message = message;
        this.payload = payload;
        this.read = read;
        this.occurredAt = occurredAt;
        this.createdAt = createdAt;
    }

    public UUID getId() { return id; }
    public String getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public String getAggregateId() { return aggregateId; }
    public String getRecipientUserId() { return recipientUserId; }
    public String getMessage() { return message; }
    public String getPayload() { return payload; }
    public boolean isRead() { return read; }
    public void setRead(boolean read) { this.read = read; }
    public Instant getOccurredAt() { return occurredAt; }
    public Instant getCreatedAt() { return createdAt; }
}
```

- [ ] **Step 5: Write the Spring Data repository**

```java
package com.nexus.notification.infrastructure.persistence;

import com.nexus.notification.infrastructure.persistence.entity.NotificationJpaEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface NotificationJpaRepository extends JpaRepository<NotificationJpaEntity, UUID> {
    List<NotificationJpaEntity> findByRecipientUserIdOrderByCreatedAtDesc(String recipientUserId);
    List<NotificationJpaEntity> findAllByOrderByCreatedAtDesc();
    List<NotificationJpaEntity> findAllByEventTypeOrderByCreatedAtDesc(String eventType);
    List<NotificationJpaEntity> findAllByAggregateIdOrderByCreatedAtDesc(String aggregateId);
    List<NotificationJpaEntity> findAllByEventTypeAndAggregateIdOrderByCreatedAtDesc(String eventType, String aggregateId);
}
```

- [ ] **Step 6: Write the adapter**

```java
package com.nexus.notification.infrastructure.persistence;

import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import com.nexus.notification.domain.model.Notification;
import com.nexus.notification.infrastructure.persistence.entity.NotificationJpaEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
public class NotificationRepositoryAdapter implements NotificationRepositoryPort {

    private static final Logger log = LoggerFactory.getLogger(NotificationRepositoryAdapter.class);

    private final NotificationJpaRepository repository;

    public NotificationRepositoryAdapter(NotificationJpaRepository repository) {
        this.repository = repository;
    }

    @Override
    public void record(Notification notification) {
        NotificationJpaEntity entity = toEntity(notification);
        try {
            // saveAndFlush (not save) so the unique-constraint violation, if any, surfaces
            // here and now -- a plain save() only flushes lazily, which would let a
            // duplicate escape this try/catch and surface somewhere unrelated later.
            repository.saveAndFlush(entity);
        } catch (DataIntegrityViolationException e) {
            log.info("Notification for event {} already recorded, skipping duplicate", notification.getEventId());
        }
    }

    @Override
    public void update(Notification notification) {
        repository.save(toEntity(notification));
    }

    @Override
    public List<Notification> findByRecipientUserId(String recipientUserId) {
        return repository.findByRecipientUserIdOrderByCreatedAtDesc(recipientUserId).stream()
                .map(NotificationRepositoryAdapter::toDomain).toList();
    }

    @Override
    public List<Notification> findAllFiltered(String eventType, String aggregateId) {
        List<NotificationJpaEntity> entities;
        if (eventType != null && aggregateId != null) {
            entities = repository.findAllByEventTypeAndAggregateIdOrderByCreatedAtDesc(eventType, aggregateId);
        } else if (eventType != null) {
            entities = repository.findAllByEventTypeOrderByCreatedAtDesc(eventType);
        } else if (aggregateId != null) {
            entities = repository.findAllByAggregateIdOrderByCreatedAtDesc(aggregateId);
        } else {
            entities = repository.findAllByOrderByCreatedAtDesc();
        }
        return entities.stream().map(NotificationRepositoryAdapter::toDomain).toList();
    }

    @Override
    public Optional<Notification> findById(String id) {
        return repository.findById(UUID.fromString(id)).map(NotificationRepositoryAdapter::toDomain);
    }

    private static NotificationJpaEntity toEntity(Notification n) {
        return new NotificationJpaEntity(UUID.fromString(n.getId()), n.getEventId(), n.getEventType(),
                n.getAggregateId(), n.getRecipientUserId(), n.getMessage(), n.getPayload(), n.isRead(),
                n.getOccurredAt(), n.getCreatedAt());
    }

    private static Notification toDomain(NotificationJpaEntity e) {
        return new Notification(e.getId().toString(), e.getEventId(), e.getEventType(), e.getAggregateId(),
                e.getRecipientUserId(), e.getMessage(), e.getPayload(), e.isRead(), e.getOccurredAt(), e.getCreatedAt());
    }
}
```

- [ ] **Step 7: Write the empty `UseCaseConfig` shell**

```java
package com.nexus.notification.infrastructure.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class UseCaseConfig {
}
```

- [ ] **Step 8: Write the failing test**

```java
package com.nexus.notification.infrastructure.persistence;

import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import com.nexus.notification.domain.model.Notification;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class NotificationRepositoryAdapterTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("notification_db").withUsername("nexus").withPassword("nexus");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eureka.client.enabled", () -> "false");
        registry.add("spring.kafka.bootstrap-servers", () -> "");
        registry.add("spring.autoconfigure.exclude",
                () -> "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration");
    }

    @Autowired
    private NotificationRepositoryPort repository;

    @Test
    void record_sameEventIdTwice_resultsInExactlyOneRow() {
        String eventId = UUID.randomUUID().toString();
        Notification first = Notification.record(eventId, "UserRegistered", "user-1",
                "user-1", "Welcome!", "{}", Instant.now());
        Notification duplicate = Notification.record(eventId, "UserRegistered", "user-1",
                "user-1", "Welcome!", "{}", Instant.now());

        repository.record(first);
        repository.record(duplicate);

        assertThat(repository.findByRecipientUserId("user-1")).hasSize(1);
    }

    @Test
    void findAllFiltered_byEventTypeAndAggregateId_returnsOnlyMatchingRows() {
        repository.record(Notification.record(UUID.randomUUID().toString(), "AuctionWon", "auction-1",
                "winner-1", "You won!", "{}", Instant.now()));
        repository.record(Notification.record(UUID.randomUUID().toString(), "ProductCreated", "product-1",
                null, null, "{}", Instant.now()));

        assertThat(repository.findAllFiltered("AuctionWon", null)).hasSize(1);
        assertThat(repository.findAllFiltered(null, "product-1")).hasSize(1);
        assertThat(repository.findAllFiltered(null, null)).hasSize(2);
    }
}
```

- [ ] **Step 9: Run it and confirm it fails (no DB migration path recognized yet / entity mismatch)**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=NotificationRepositoryAdapterTest`
Expected: FAILS if any step above was skipped (e.g. missing migration). If all steps above were followed in order, this should already PASS — in which case continue to Step 10 anyway to confirm.

- [ ] **Step 10: Run the test and confirm it passes**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=NotificationRepositoryAdapterTest`
Expected: `BUILD SUCCESS`, both tests pass. (Requires Docker running — Testcontainers starts a real Postgres container.)

- [ ] **Step 11: Commit**

```bash
git add src/main/resources/db/migration/V1__create_notifications_table.sql \
  src/main/java/com/nexus/notification/domain/model/Notification.java \
  src/main/java/com/nexus/notification/application/port/out/NotificationRepositoryPort.java \
  src/main/java/com/nexus/notification/infrastructure/persistence/entity/NotificationJpaEntity.java \
  src/main/java/com/nexus/notification/infrastructure/persistence/NotificationJpaRepository.java \
  src/main/java/com/nexus/notification/infrastructure/persistence/NotificationRepositoryAdapter.java \
  src/main/java/com/nexus/notification/infrastructure/config/UseCaseConfig.java \
  src/test/java/com/nexus/notification/infrastructure/persistence/NotificationRepositoryAdapterTest.java
git commit -m "feat: add notifications table and idempotent repository adapter"
git push origin main
```

---

### Task 3: `NotificationMessageResolver` — maps 5 known event types to (recipient, message)

**Files:**
- Create: `notification-service/src/main/java/com/nexus/notification/application/service/NotificationMessageResolver.java`
- Test: `notification-service/src/test/java/com/nexus/notification/application/service/NotificationMessageResolverTest.java`

**Interfaces:**
- Produces: `NotificationMessageResolver` with `Resolution resolve(String eventType, DomainEvent event)`, where `Resolution` is a nested record `(String recipientUserId, String message)` and `Resolution.UNMAPPED` is the `(null, null)` constant.
- Consumes: nothing from earlier tasks (pure, no Spring context needed — plain JUnit).

- [ ] **Step 1: Write the failing test**

```java
package com.nexus.notification.application.service;

import com.nexus.common.events.*;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationMessageResolverTest {

    private final NotificationMessageResolver resolver = new NotificationMessageResolver();

    @Test
    void resolve_userRegistered_returnsUserIdAndWelcomeMessage() {
        UserRegisteredEvent event = new UserRegisteredEvent("user-1", "alice@example.com", "Alice");

        NotificationMessageResolver.Resolution resolution = resolver.resolve("UserRegistered", event);

        assertThat(resolution.recipientUserId()).isEqualTo("user-1");
        assertThat(resolution.message()).contains("Alice");
    }

    @Test
    void resolve_bidPlaced_returnsBidderIdNotAggregateId() {
        BidPlacedEvent event = new BidPlacedEvent("auction-1", "bidder-1", new BigDecimal("150.00"));

        NotificationMessageResolver.Resolution resolution = resolver.resolve("BidPlaced", event);

        assertThat(resolution.recipientUserId()).isEqualTo("bidder-1");
        assertThat(resolution.message()).contains("150.00").contains("auction-1");
    }

    @Test
    void resolve_outbid_returnsOutbidBidderId() {
        OutbidEvent event = new OutbidEvent("auction-1", "outbid-bidder", new BigDecimal("200.00"));

        NotificationMessageResolver.Resolution resolution = resolver.resolve("Outbid", event);

        assertThat(resolution.recipientUserId()).isEqualTo("outbid-bidder");
    }

    @Test
    void resolve_auctionWon_returnsWinnerId() {
        AuctionWonEvent event = new AuctionWonEvent("auction-1", "product-1", "seller-1", "winner-1", new BigDecimal("500.00"));

        NotificationMessageResolver.Resolution resolution = resolver.resolve("AuctionWon", event);

        assertThat(resolution.recipientUserId()).isEqualTo("winner-1");
        assertThat(resolution.message()).contains("500.00");
    }

    @Test
    void resolve_auctionSettled_returnsWinnerId() {
        AuctionSettledEvent event = new AuctionSettledEvent("auction-1", "winner-1", new BigDecimal("500.00"), Instant.now());

        NotificationMessageResolver.Resolution resolution = resolver.resolve("AuctionSettled", event);

        assertThat(resolution.recipientUserId()).isEqualTo("winner-1");
    }

    @Test
    void resolve_unmappedEventType_returnsUnmapped() {
        NotificationMessageResolver.Resolution resolution = resolver.resolve("ProductCreated", null);

        assertThat(resolution).isEqualTo(NotificationMessageResolver.Resolution.UNMAPPED);
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=NotificationMessageResolverTest`
Expected: FAIL with "class NotificationMessageResolver not found" (or similar compile error).

- [ ] **Step 3: Write the implementation**

```java
package com.nexus.notification.application.service;

import com.nexus.common.events.*;

public class NotificationMessageResolver {

    public record Resolution(String recipientUserId, String message) {
        public static final Resolution UNMAPPED = new Resolution(null, null);
    }

    public Resolution resolve(String eventType, DomainEvent event) {
        return switch (eventType) {
            case "UserRegistered" -> resolveUserRegistered((UserRegisteredEvent) event);
            case "BidPlaced" -> resolveBidPlaced((BidPlacedEvent) event);
            case "Outbid" -> resolveOutbid((OutbidEvent) event);
            case "AuctionWon" -> resolveAuctionWon((AuctionWonEvent) event);
            case "AuctionSettled" -> resolveAuctionSettled((AuctionSettledEvent) event);
            default -> Resolution.UNMAPPED;
        };
    }

    private Resolution resolveUserRegistered(UserRegisteredEvent e) {
        return new Resolution(e.getUserId(), "Chào mừng " + e.getFullName() + " đã đăng ký tài khoản thành công!");
    }

    private Resolution resolveBidPlaced(BidPlacedEvent e) {
        return new Resolution(e.getBidderId(),
                "Bạn đã đặt giá " + e.getAmount() + " cho phiên đấu giá " + e.getAuctionId());
    }

    private Resolution resolveOutbid(OutbidEvent e) {
        return new Resolution(e.getOutbidBidderId(),
                "Bạn đã bị vượt giá trong phiên đấu giá " + e.getAuctionId()
                        + ", giá cao nhất hiện tại là " + e.getNewHighestBid());
    }

    private Resolution resolveAuctionWon(AuctionWonEvent e) {
        return new Resolution(e.getWinnerId(),
                "Chúc mừng! Bạn đã thắng phiên đấu giá " + e.getAuctionId() + " với giá " + e.getFinalPrice());
    }

    private Resolution resolveAuctionSettled(AuctionSettledEvent e) {
        return new Resolution(e.getWinnerId(),
                "Phiên đấu giá " + e.getAuctionId() + " đã hoàn tất thanh toán với giá " + e.getFinalPrice());
    }
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=NotificationMessageResolverTest`
Expected: `BUILD SUCCESS`, 6 tests pass. No Docker needed — this is a plain JUnit test, no Spring context.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/nexus/notification/application/service/NotificationMessageResolver.java \
  src/test/java/com/nexus/notification/application/service/NotificationMessageResolverTest.java
git commit -m "feat: add NotificationMessageResolver for the 5 known event types"
git push origin main
```

---

### Task 4: `RecordNotificationUseCase` — ties the resolver and repository together

**Files:**
- Create: `notification-service/src/main/java/com/nexus/notification/application/usecase/RecordNotificationUseCase.java`
- Modify: `notification-service/src/main/java/com/nexus/notification/infrastructure/config/UseCaseConfig.java`
- Test: `notification-service/src/test/java/com/nexus/notification/application/usecase/RecordNotificationUseCaseIntegrationTest.java`

**Interfaces:**
- Consumes: `NotificationRepositoryPort` (Task 2), `NotificationMessageResolver` (Task 3).
- Produces: `RecordNotificationUseCase` with `void record(String eventId, String eventType, String aggregateId, Instant occurredAt, String rawPayload, DomainEvent typedEventOrNull)`. This is the method Task 5's listeners call — the signature here is final for this plan.

- [ ] **Step 1: Write the failing test**

```java
package com.nexus.notification.application.usecase;

import com.nexus.common.events.UserRegisteredEvent;
import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import com.nexus.notification.application.service.NotificationMessageResolver;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RecordNotificationUseCaseIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("notification_db").withUsername("nexus").withPassword("nexus");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eureka.client.enabled", () -> "false");
        registry.add("spring.autoconfigure.exclude",
                () -> "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration");
    }

    @Autowired private RecordNotificationUseCase recordNotificationUseCase;
    @Autowired private NotificationRepositoryPort repository;

    @Test
    void record_mappedEventType_derivesRecipientAndMessage() {
        UserRegisteredEvent event = new UserRegisteredEvent("user-1", "alice@example.com", "Alice");

        recordNotificationUseCase.record(event.getEventId(), "UserRegistered", "user-1",
                Instant.now(), "{\"eventType\":\"UserRegistered\"}", event);

        assertThat(repository.findByRecipientUserId("user-1")).hasSize(1);
        assertThat(repository.findByRecipientUserId("user-1").get(0).getMessage()).contains("Alice");
    }

    @Test
    void record_unmappedEventType_stillStoresRowWithNullRecipientAndMessage() {
        String eventId = UUID.randomUUID().toString();

        recordNotificationUseCase.record(eventId, "ProductCreated", "product-1",
                Instant.now(), "{\"eventType\":\"ProductCreated\"}", null);

        var all = repository.findAllFiltered("ProductCreated", null);
        assertThat(all).hasSize(1);
        assertThat(all.get(0).getRecipientUserId()).isNull();
        assertThat(all.get(0).getMessage()).isNull();
        assertThat(all.get(0).getPayload()).isEqualTo("{\"eventType\":\"ProductCreated\"}");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=RecordNotificationUseCaseIntegrationTest`
Expected: FAIL — `RecordNotificationUseCase` bean not found (doesn't exist yet).

- [ ] **Step 3: Write the use case**

```java
package com.nexus.notification.application.usecase;

import com.nexus.common.events.DomainEvent;
import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import com.nexus.notification.application.service.NotificationMessageResolver;
import com.nexus.notification.domain.model.Notification;

import java.time.Instant;

public class RecordNotificationUseCase {

    private final NotificationRepositoryPort repository;
    private final NotificationMessageResolver resolver;

    public RecordNotificationUseCase(NotificationRepositoryPort repository, NotificationMessageResolver resolver) {
        this.repository = repository;
        this.resolver = resolver;
    }

    public void record(String eventId, String eventType, String aggregateId, Instant occurredAt,
                        String rawPayload, DomainEvent typedEventOrNull) {
        NotificationMessageResolver.Resolution resolution = typedEventOrNull == null
                ? NotificationMessageResolver.Resolution.UNMAPPED
                : resolver.resolve(eventType, typedEventOrNull);

        Notification notification = Notification.record(eventId, eventType, aggregateId,
                resolution.recipientUserId(), resolution.message(), rawPayload, occurredAt);
        repository.record(notification);
    }
}
```

- [ ] **Step 4: Wire it into `UseCaseConfig`**

```java
package com.nexus.notification.infrastructure.config;

import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import com.nexus.notification.application.service.NotificationMessageResolver;
import com.nexus.notification.application.usecase.RecordNotificationUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UseCaseConfig {

    @Bean
    public NotificationMessageResolver notificationMessageResolver() {
        return new NotificationMessageResolver();
    }

    @Bean
    public RecordNotificationUseCase recordNotificationUseCase(NotificationRepositoryPort repository,
                                                                  NotificationMessageResolver resolver) {
        return new RecordNotificationUseCase(repository, resolver);
    }
}
```

- [ ] **Step 5: Run it to verify it passes**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=RecordNotificationUseCaseIntegrationTest`
Expected: `BUILD SUCCESS`, both tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/nexus/notification/application/usecase/RecordNotificationUseCase.java \
  src/main/java/com/nexus/notification/infrastructure/config/UseCaseConfig.java \
  src/test/java/com/nexus/notification/application/usecase/RecordNotificationUseCaseIntegrationTest.java
git commit -m "feat: add RecordNotificationUseCase wiring resolver and repository"
git push origin main
```

---

### Task 5: The 3 Kafka listeners

**Files:**
- Create: `notification-service/src/main/java/com/nexus/notification/infrastructure/messaging/EventRecordingService.java`
- Create: `notification-service/src/main/java/com/nexus/notification/infrastructure/messaging/UserEventsListener.java`
- Create: `notification-service/src/main/java/com/nexus/notification/infrastructure/messaging/CatalogEventsListener.java`
- Create: `notification-service/src/main/java/com/nexus/notification/infrastructure/messaging/AuctionEventsListener.java`
- Test: `notification-service/src/test/java/com/nexus/notification/infrastructure/messaging/AuctionEventsListenerIntegrationTest.java`

**Interfaces:**
- Consumes: `RecordNotificationUseCase.record(...)` (Task 4).
- Produces: nothing further tasks depend on — this is the top-level inbound adapter.

- [ ] **Step 1: Write the shared parsing/dispatch service**

```java
package com.nexus.notification.infrastructure.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.common.events.DomainEvent;
import com.nexus.notification.application.usecase.RecordNotificationUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

@Component
public class EventRecordingService {

    private static final Logger log = LoggerFactory.getLogger(EventRecordingService.class);

    private final ObjectMapper objectMapper;
    private final RecordNotificationUseCase recordNotificationUseCase;

    public EventRecordingService(ObjectMapper objectMapper, RecordNotificationUseCase recordNotificationUseCase) {
        this.objectMapper = objectMapper;
        this.recordNotificationUseCase = recordNotificationUseCase;
    }

    public void process(String rawPayload, Map<String, Class<? extends DomainEvent>> mappedTypes) {
        try {
            JsonNode node = objectMapper.readTree(rawPayload);
            String eventId = node.get("eventId").asText();
            String eventType = node.get("eventType").asText();
            String aggregateId = node.hasNonNull("aggregateId") ? node.get("aggregateId").asText() : null;
            Instant occurredAt = Instant.parse(node.get("occurredAt").asText());

            Class<? extends DomainEvent> mappedClass = mappedTypes.get(eventType);
            DomainEvent typedEvent = mappedClass == null ? null : objectMapper.treeToValue(node, mappedClass);

            recordNotificationUseCase.record(eventId, eventType, aggregateId, occurredAt, rawPayload, typedEvent);
        } catch (Exception e) {
            // Deliberately broad: a malformed message on the topic must not stop this
            // listener's partition from consuming later messages.
            log.error("Failed to process event payload, skipping: {}", rawPayload, e);
        }
    }
}
```

- [ ] **Step 2: Write the 3 listeners**

```java
package com.nexus.notification.infrastructure.messaging;

import com.nexus.common.events.DomainEvent;
import com.nexus.common.events.UserRegisteredEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class UserEventsListener {

    private static final Map<String, Class<? extends DomainEvent>> MAPPED_TYPES =
            Map.of("UserRegistered", UserRegisteredEvent.class);

    private final EventRecordingService eventRecordingService;

    public UserEventsListener(EventRecordingService eventRecordingService) {
        this.eventRecordingService = eventRecordingService;
    }

    @KafkaListener(topics = "user-events")
    public void onMessage(String rawPayload) {
        eventRecordingService.process(rawPayload, MAPPED_TYPES);
    }
}
```

```java
package com.nexus.notification.infrastructure.messaging;

import com.nexus.common.events.DomainEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CatalogEventsListener {

    // None of the 5 mapped event types come from catalog-service -- every row recorded
    // from this topic is audit-only (null recipient/message) per the spec's scope.
    private static final Map<String, Class<? extends DomainEvent>> MAPPED_TYPES = Map.of();

    private final EventRecordingService eventRecordingService;

    public CatalogEventsListener(EventRecordingService eventRecordingService) {
        this.eventRecordingService = eventRecordingService;
    }

    @KafkaListener(topics = "catalog-events")
    public void onMessage(String rawPayload) {
        eventRecordingService.process(rawPayload, MAPPED_TYPES);
    }
}
```

```java
package com.nexus.notification.infrastructure.messaging;

import com.nexus.common.events.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class AuctionEventsListener {

    private static final Map<String, Class<? extends DomainEvent>> MAPPED_TYPES = Map.of(
            "BidPlaced", BidPlacedEvent.class,
            "Outbid", OutbidEvent.class,
            "AuctionWon", AuctionWonEvent.class,
            "AuctionSettled", AuctionSettledEvent.class);

    private final EventRecordingService eventRecordingService;

    public AuctionEventsListener(EventRecordingService eventRecordingService) {
        this.eventRecordingService = eventRecordingService;
    }

    @KafkaListener(topics = "auction-events")
    public void onMessage(String rawPayload) {
        eventRecordingService.process(rawPayload, MAPPED_TYPES);
    }
}
```

- [ ] **Step 3: Write the failing integration test**

```java
package com.nexus.notification.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.common.events.AuctionWonEvent;
import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class AuctionEventsListenerIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("notification_db").withUsername("nexus").withPassword("nexus");

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

    @Autowired private NotificationRepositoryPort repository;
    @Autowired private ObjectMapper objectMapper;

    @Test
    void consumingAuctionWonEvent_recordsNotificationForWinner() throws Exception {
        AuctionWonEvent event = new AuctionWonEvent("auction-1", "product-1", "seller-1", "winner-1", new BigDecimal("500.00"));
        produce("auction-events", event.getAggregateId(), objectMapper.writeValueAsString(event));

        List<com.nexus.notification.domain.model.Notification> found = pollUntilFound("winner-1", Duration.ofSeconds(10));

        assertThat(found).hasSize(1);
        assertThat(found.get(0).getMessage()).contains("500.00");
    }

    @Test
    void consumingMalformedPayload_doesNotCrashListener_andLaterValidMessageStillProcessed() throws Exception {
        produce("auction-events", "bad-key", "{not valid json");

        AuctionWonEvent event = new AuctionWonEvent("auction-2", "product-2", "seller-2", "winner-2", new BigDecimal("77.00"));
        produce("auction-events", event.getAggregateId(), objectMapper.writeValueAsString(event));

        List<com.nexus.notification.domain.model.Notification> found = pollUntilFound("winner-2", Duration.ofSeconds(10));

        assertThat(found).hasSize(1);
    }

    private void produce(String topic, String key, String value) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<com.nexus.notification.domain.model.Notification> pollUntilFound(String recipientUserId, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            List<com.nexus.notification.domain.model.Notification> found = repository.findByRecipientUserId(recipientUserId);
            if (!found.isEmpty()) {
                return found;
            }
            Thread.sleep(200);
        }
        return repository.findByRecipientUserId(recipientUserId);
    }
}
```

- [ ] **Step 4: Run it to verify it fails**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=AuctionEventsListenerIntegrationTest`
Expected: FAIL — listener classes don't exist yet (if run before Step 1-2 are saved) or times out with empty list (if listeners aren't registered).

- [ ] **Step 5: Run it again after Steps 1-2 are in place, verify it passes**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=AuctionEventsListenerIntegrationTest`
Expected: `BUILD SUCCESS`, both tests pass. The second test specifically proves a malformed message on the topic does not block the valid message sent right after it.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/nexus/notification/infrastructure/messaging/
git add src/test/java/com/nexus/notification/infrastructure/messaging/AuctionEventsListenerIntegrationTest.java
git commit -m "feat: add 3 Kafka listeners (user/catalog/auction-events)"
git push origin main
```

---

### Task 6: `SecurityConfig` — JWT-authenticated by default, no public GET

**Files:**
- Create: `notification-service/src/main/java/com/nexus/notification/infrastructure/config/SecurityConfig.java`

**Interfaces:**
- Produces: a `SecurityFilterChain` bean requiring authentication on everything under `/api/v1/**`, `/actuator/**` open. Required by Task 7's controller tests.
- Consumes: `JwtAuthenticationFilter` (from `common-security`, auto-wired by Spring).

- [ ] **Step 1: Write `SecurityConfig`**

```java
package com.nexus.notification.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexus.common.core.ApiError;
import com.nexus.common.core.ApiResponse;
import com.nexus.common.security.JwtAuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

    // NOTE: unlike every other SecurityConfig in this codebase, there is no
    // `.requestMatchers(HttpMethod.GET, "/api/v1/**").permitAll()` here. Every endpoint
    // in this service is either a specific user's own data or privileged audit data --
    // see the plan's Global Constraints section for why this diverges from the usual
    // convention.
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http, AuthenticationEntryPoint authenticationEntryPoint) throws Exception {
        http.csrf(csrf -> csrf.disable())
            .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .exceptionHandling(handling -> handling.authenticationEntryPoint(authenticationEntryPoint))
            .authorizeHttpRequests(auth -> auth
                    .requestMatchers("/actuator/**").permitAll()
                    .anyRequest().authenticated())
            .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }
}
```

- [ ] **Step 2: Verify it compiles (no test yet — exercised by Task 7's controller tests)**

Run: `cd /c/FPT/notification-service && mvn -q compile`
Expected: exit code 0.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/nexus/notification/infrastructure/config/SecurityConfig.java
git commit -m "feat: add SecurityConfig requiring authentication on all API routes"
git push origin main
```

---

### Task 7: Read/mark-read use cases, REST controller, DTOs — the public API surface

**Files:**
- Create: `notification-service/src/main/java/com/nexus/notification/application/usecase/ListMyNotificationsUseCase.java`
- Create: `notification-service/src/main/java/com/nexus/notification/application/usecase/ListAllNotificationsUseCase.java`
- Create: `notification-service/src/main/java/com/nexus/notification/application/usecase/MarkNotificationReadUseCase.java`
- Modify: `notification-service/src/main/java/com/nexus/notification/infrastructure/config/UseCaseConfig.java`
- Create: `notification-service/src/main/java/com/nexus/notification/api/dto/NotificationResponse.java`
- Create: `notification-service/src/main/java/com/nexus/notification/api/mapper/NotificationApiMapper.java`
- Create: `notification-service/src/main/java/com/nexus/notification/api/NotificationController.java`
- Test: `notification-service/src/test/java/com/nexus/notification/api/NotificationControllerTest.java`

**Interfaces:**
- Consumes: `NotificationRepositoryPort` (Task 2).
- Produces: `GET /api/v1/notifications/me`, `GET /api/v1/notifications`, `PATCH /api/v1/notifications/{id}/read`.

- [ ] **Step 1: Write the 3 use cases**

```java
package com.nexus.notification.application.usecase;

import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import com.nexus.notification.domain.model.Notification;

import java.util.List;

public class ListMyNotificationsUseCase {
    private final NotificationRepositoryPort repository;

    public ListMyNotificationsUseCase(NotificationRepositoryPort repository) {
        this.repository = repository;
    }

    public List<Notification> list(String recipientUserId) {
        return repository.findByRecipientUserId(recipientUserId);
    }
}
```

```java
package com.nexus.notification.application.usecase;

import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import com.nexus.notification.domain.model.Notification;

import java.util.List;

public class ListAllNotificationsUseCase {
    private final NotificationRepositoryPort repository;

    public ListAllNotificationsUseCase(NotificationRepositoryPort repository) {
        this.repository = repository;
    }

    public List<Notification> list(String eventType, String aggregateId) {
        return repository.findAllFiltered(eventType, aggregateId);
    }
}
```

```java
package com.nexus.notification.application.usecase;

import com.nexus.common.core.exception.ForbiddenException;
import com.nexus.common.core.exception.NotFoundException;
import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import com.nexus.notification.domain.model.Notification;

import java.util.Objects;

public class MarkNotificationReadUseCase {
    private final NotificationRepositoryPort repository;

    public MarkNotificationReadUseCase(NotificationRepositoryPort repository) {
        this.repository = repository;
    }

    public void markRead(String notificationId, String callerUserId) {
        Notification notification = repository.findById(notificationId)
                .orElseThrow(() -> new NotFoundException("NOTIFICATION_NOT_FOUND",
                        "Notification not found: " + notificationId));

        if (!Objects.equals(notification.getRecipientUserId(), callerUserId)) {
            throw new ForbiddenException("NOT_YOUR_NOTIFICATION", "You do not own this notification");
        }

        notification.markRead();
        repository.update(notification);
    }
}
```

- [ ] **Step 2: Add the 3 new beans to `UseCaseConfig`**

```java
package com.nexus.notification.infrastructure.config;

import com.nexus.notification.application.port.out.NotificationRepositoryPort;
import com.nexus.notification.application.service.NotificationMessageResolver;
import com.nexus.notification.application.usecase.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UseCaseConfig {

    @Bean
    public NotificationMessageResolver notificationMessageResolver() {
        return new NotificationMessageResolver();
    }

    @Bean
    public RecordNotificationUseCase recordNotificationUseCase(NotificationRepositoryPort repository,
                                                                  NotificationMessageResolver resolver) {
        return new RecordNotificationUseCase(repository, resolver);
    }

    @Bean
    public ListMyNotificationsUseCase listMyNotificationsUseCase(NotificationRepositoryPort repository) {
        return new ListMyNotificationsUseCase(repository);
    }

    @Bean
    public ListAllNotificationsUseCase listAllNotificationsUseCase(NotificationRepositoryPort repository) {
        return new ListAllNotificationsUseCase(repository);
    }

    @Bean
    public MarkNotificationReadUseCase markNotificationReadUseCase(NotificationRepositoryPort repository) {
        return new MarkNotificationReadUseCase(repository);
    }
}
```

- [ ] **Step 3: Write the DTO and mapper**

```java
package com.nexus.notification.api.dto;

import java.time.Instant;

public record NotificationResponse(
        String id, String eventType, String aggregateId, String recipientUserId,
        String message, boolean read, String payload, Instant occurredAt) {
}
```

```java
package com.nexus.notification.api.mapper;

import com.nexus.notification.api.dto.NotificationResponse;
import com.nexus.notification.domain.model.Notification;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface NotificationApiMapper {
    NotificationResponse toResponse(Notification notification);
}
```

- [ ] **Step 4: Write the controller**

```java
package com.nexus.notification.api;

import com.nexus.notification.api.dto.NotificationResponse;
import com.nexus.notification.api.mapper.NotificationApiMapper;
import com.nexus.notification.application.usecase.ListAllNotificationsUseCase;
import com.nexus.notification.application.usecase.ListMyNotificationsUseCase;
import com.nexus.notification.application.usecase.MarkNotificationReadUseCase;
import com.nexus.common.core.ApiResponse;
import com.nexus.common.security.RequiresPrivilege;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/notifications")
public class NotificationController {

    private final ListMyNotificationsUseCase listMyNotificationsUseCase;
    private final ListAllNotificationsUseCase listAllNotificationsUseCase;
    private final MarkNotificationReadUseCase markNotificationReadUseCase;
    private final NotificationApiMapper mapper;

    public NotificationController(ListMyNotificationsUseCase listMyNotificationsUseCase,
                                   ListAllNotificationsUseCase listAllNotificationsUseCase,
                                   MarkNotificationReadUseCase markNotificationReadUseCase,
                                   NotificationApiMapper mapper) {
        this.listMyNotificationsUseCase = listMyNotificationsUseCase;
        this.listAllNotificationsUseCase = listAllNotificationsUseCase;
        this.markNotificationReadUseCase = markNotificationReadUseCase;
        this.mapper = mapper;
    }

    @GetMapping("/me")
    public ResponseEntity<ApiResponse<List<NotificationResponse>>> listMine(Authentication authentication) {
        List<NotificationResponse> results = listMyNotificationsUseCase.list(callerId(authentication))
                .stream().map(mapper::toResponse).toList();
        return ResponseEntity.ok(ApiResponse.ok(results));
    }

    @RequiresPrivilege("NOTIFICATION.AUDIT")
    @GetMapping
    public ResponseEntity<ApiResponse<List<NotificationResponse>>> listAll(
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) String aggregateId) {
        List<NotificationResponse> results = listAllNotificationsUseCase.list(eventType, aggregateId)
                .stream().map(mapper::toResponse).toList();
        return ResponseEntity.ok(ApiResponse.ok(results));
    }

    @PatchMapping("/{id}/read")
    public ResponseEntity<ApiResponse<Void>> markRead(Authentication authentication, @PathVariable String id) {
        markNotificationReadUseCase.markRead(id, callerId(authentication));
        return ResponseEntity.ok(ApiResponse.ok(null));
    }

    private static String callerId(Authentication authentication) {
        return (String) authentication.getPrincipal();
    }
}
```

- [ ] **Step 5: Write the failing controller test**

```java
package com.nexus.notification.api;

import com.nexus.notification.api.mapper.NotificationApiMapperImpl;
import com.nexus.notification.application.usecase.ListAllNotificationsUseCase;
import com.nexus.notification.application.usecase.ListMyNotificationsUseCase;
import com.nexus.notification.application.usecase.MarkNotificationReadUseCase;
import com.nexus.notification.domain.model.Notification;
import com.nexus.notification.infrastructure.config.SecurityConfig;
import com.nexus.common.core.exception.ForbiddenException;
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

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(NotificationController.class)
@ImportAutoConfiguration(AopAutoConfiguration.class)
@Import({GlobalExceptionHandler.class, NotificationApiMapperImpl.class, SecurityConfig.class,
        JwtAuthenticationFilter.class, PrivilegeAuthorizationAspect.class})
class NotificationControllerTest {

    @Autowired private MockMvc mockMvc;

    @MockBean private ListMyNotificationsUseCase listMyNotificationsUseCase;
    @MockBean private ListAllNotificationsUseCase listAllNotificationsUseCase;
    @MockBean private MarkNotificationReadUseCase markNotificationReadUseCase;
    @MockBean private JwtTokenProvider jwtTokenProvider;

    private void mockValidToken(String userId, List<String> privileges) {
        when(jwtTokenProvider.isValid("good-token")).thenReturn(true);
        Claims claims = Jwts.claims().subject(userId).add("privileges", privileges).build();
        when(jwtTokenProvider.parseClaims("good-token")).thenReturn(claims);
    }

    @Test
    void listMine_returns401WithoutToken() throws Exception {
        mockMvc.perform(get("/api/v1/notifications/me"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void listMine_returns200WithOnlyCallersNotifications() throws Exception {
        mockValidToken("user-1", List.of());
        when(listMyNotificationsUseCase.list("user-1")).thenReturn(List.of(
                new Notification("n-1", "e-1", "UserRegistered", "user-1", "user-1", "Welcome!", "{}",
                        false, Instant.now(), Instant.now())));

        mockMvc.perform(get("/api/v1/notifications/me").header("Authorization", "Bearer good-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].recipientUserId").value("user-1"));
    }

    @Test
    void listAll_returns403WithoutAuditPrivilege() throws Exception {
        mockValidToken("user-1", List.of());

        mockMvc.perform(get("/api/v1/notifications").header("Authorization", "Bearer good-token"))
                .andExpect(status().isForbidden());
    }

    @Test
    void listAll_returns200WithAuditPrivilege() throws Exception {
        mockValidToken("admin-1", List.of("NOTIFICATION.AUDIT"));
        when(listAllNotificationsUseCase.list(null, null)).thenReturn(List.of());

        mockMvc.perform(get("/api/v1/notifications").header("Authorization", "Bearer good-token"))
                .andExpect(status().isOk());
    }

    @Test
    void markRead_returns403WhenCallerDoesNotOwnTheNotification() throws Exception {
        mockValidToken("user-1", List.of());
        org.mockito.Mockito.doThrow(new ForbiddenException("NOT_YOUR_NOTIFICATION", "You do not own this notification"))
                .when(markNotificationReadUseCase).markRead(any(), any());

        mockMvc.perform(patch("/api/v1/notifications/n-1/read").header("Authorization", "Bearer good-token"))
                .andExpect(status().isForbidden());
    }

    @Test
    void markRead_returns200WhenCallerOwnsTheNotification() throws Exception {
        mockValidToken("user-1", List.of());

        mockMvc.perform(patch("/api/v1/notifications/n-1/read").header("Authorization", "Bearer good-token"))
                .andExpect(status().isOk());
    }
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=NotificationControllerTest`
Expected: FAIL — controller/mapper/DTO classes don't exist yet (if run before Steps 1-4 are saved).

- [ ] **Step 7: Run it again with Steps 1-4 in place, verify it passes**

Run: `cd /c/FPT/notification-service && mvn -q test -Dtest=NotificationControllerTest`
Expected: `BUILD SUCCESS`, all 6 tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/nexus/notification/application/usecase/ListMyNotificationsUseCase.java \
  src/main/java/com/nexus/notification/application/usecase/ListAllNotificationsUseCase.java \
  src/main/java/com/nexus/notification/application/usecase/MarkNotificationReadUseCase.java \
  src/main/java/com/nexus/notification/infrastructure/config/UseCaseConfig.java \
  src/main/java/com/nexus/notification/api/ \
  src/test/java/com/nexus/notification/api/NotificationControllerTest.java
git commit -m "feat: add notification read/audit/mark-read API"
git push origin main
```

---

### Task 8: Cross-repo wiring — docker-compose, build-all.sh, gateway route, privilege seed

**Files:**
- Modify: `infra/docker-compose.yml`
- Modify: `infra/build-all.sh`
- Modify: `api-gateway/src/main/resources/application.yml`
- Create: `user-service/src/main/resources/db/migration/V7__seed_notification_privileges.sql`

**Interfaces:**
- Consumes: nothing code-level — this task only changes configuration/infrastructure files in 3 other repos.

- [ ] **Step 1: Add `postgres-notification` and `notification-service` to `infra/docker-compose.yml`**

Add after the existing `catalog-service` block (before `auction-service`, alphabetical-ish grouping already used) — actually simplest is to append both new blocks at the end of the file, matching how `auction-service` was appended after `catalog-service` previously. Insert the new Postgres container in the Postgres group (after `postgres-auction`, before `kafka`), and the new app container at the end (after `auction-service`):

```yaml
  postgres-notification:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: notification_db
      POSTGRES_USER: nexus
      POSTGRES_PASSWORD: nexus
    ports:
      - "5435:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U nexus -d notification_db"]
      interval: 5s
      timeout: 5s
      retries: 10
```

```yaml
  notification-service:
    build:
      context: ../notification-service
    depends_on:
      postgres-notification:
        condition: service_healthy
      discovery-server:
        condition: service_started
      kafka:
        condition: service_started
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres-notification:5432/notification_db
      EUREKA_CLIENT_SERVICEURL_DEFAULTZONE: http://discovery-server:8761/eureka
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    ports:
      - "8084:8084"
```

- [ ] **Step 2: Verify the compose file is still valid YAML**

Run: `cd /c/FPT/infra && docker compose config --quiet`
Expected: exit code 0, no output.

- [ ] **Step 3: Add `notification-service` to `infra/build-all.sh`'s two loops**

In `infra/build-all.sh`, change both occurrences of the repo list:

```bash
for repo in common-libs discovery-server api-gateway user-service catalog-service auction-service notification-service; do
```
and
```bash
for repo in discovery-server api-gateway user-service catalog-service auction-service notification-service; do
```

- [ ] **Step 4: Add the gateway route**

In `api-gateway/src/main/resources/application.yml`, add a new route entry after `auction-service`'s:

```yaml
        - id: notification-service
          uri: lb://notification-service
          predicates:
            - Path=/api/v1/notifications/**
```

- [ ] **Step 5: Verify the gateway's yml parses and the project still compiles**

Run: `cd /c/FPT/api-gateway && mvn -q compile`
Expected: exit code 0.

- [ ] **Step 6: Write `user-service`'s privilege-seed migration**

```sql
-- V7__seed_notification_privileges.sql
-- notification-service's NotificationController enforces this via @RequiresPrivilege
-- on GET /api/v1/notifications (the audit endpoint). /me and /{id}/read need no
-- privilege beyond being authenticated, matching how the audit-only distinction
-- is drawn in the notification-service design spec.
INSERT INTO privileges (code) VALUES ('NOTIFICATION.AUDIT');

INSERT INTO role_privileges (role_id, privilege_id)
SELECT (SELECT id FROM roles WHERE code = 'ADMIN'), id
FROM privileges
WHERE code = 'NOTIFICATION.AUDIT';
```

- [ ] **Step 7: Verify the new migration applies cleanly alongside the existing ones**

Run: `cd /c/FPT/user-service && mvn -q test`
Expected: `BUILD SUCCESS` — the existing Testcontainers-backed integration tests in `user-service` run all migrations (`V1` through the new `V7`) against a real Postgres on every run; a syntax error or ordering problem in `V7` would fail the whole suite here.

- [ ] **Step 8: Commit each repo separately**

```bash
cd /c/FPT/infra
git add docker-compose.yml build-all.sh
git commit -m "feat: wire notification-service into the compose cluster and build script"
git push origin main
```

```bash
cd /c/FPT/api-gateway
git add src/main/resources/application.yml
git commit -m "feat: route /api/v1/notifications/** to notification-service"
git push origin main
```

```bash
cd /c/FPT/user-service
git add src/main/resources/db/migration/V7__seed_notification_privileges.sql
git commit -m "feat: seed NOTIFICATION.AUDIT privilege for ADMIN"
git push origin main
```

---

### Task 9: End-to-end manual verification

**Files:** none — this task runs the already-built system, no new code.

- [ ] **Step 1: Build everything including the new service**

Run: `cd /c/FPT/infra && ./build-all.sh && docker compose build && docker compose up -d`
Expected: 11 containers now (the original 9 plus `postgres-notification` and `notification-service`). Verify with `docker ps` — all should show `Up` within ~30s (`postgres-notification` as `Up (healthy)`).

- [ ] **Step 2: Trigger a mapped event (user registration) and confirm it is recorded**

Run (adjust the token if the gateway requires one — registration does not):
```bash
curl.exe -X POST http://localhost:8080/api/v1/users/register -H "Content-Type: application/json" -d '{\"email\":\"notif-demo@fpt.edu.vn\",\"password\":\"Demo@123456\",\"fullName\":\"Notif Demo\"}'
```
Wait up to 5 seconds (outbox relay) + a few seconds (consumer), then check via DBeaver or:
```bash
docker exec -it infra-postgres-notification-1 psql -U nexus -d notification_db -c "SELECT event_type, recipient_user_id, message FROM notifications ORDER BY created_at DESC LIMIT 5;"
```
Expected: a row with `event_type = UserRegistered`, `recipient_user_id` equal to the new user's id, and a `message` containing "Notif Demo".

- [ ] **Step 3: Confirm the personal inbox API returns it**

Log in as that user (`POST /api/v1/auth/login`) to get a JWT, then:
```bash
curl.exe http://localhost:8080/api/v1/notifications/me -H "Authorization: Bearer <token>"
```
Expected: `200`, `data` array with the same notification.

- [ ] **Step 4: Confirm the audit endpoint is privilege-gated**

With the same non-admin token:
```bash
curl.exe http://localhost:8080/api/v1/notifications -H "Authorization: Bearer <token>"
```
Expected: `403`. Then with an ADMIN account's token, expect `200` with every recorded event (including ones from `catalog-events`/other `auction-events` types with `null` `message`).

---

## Self-Review

**Spec coverage:** every "In scope" bullet of the design spec maps to a task — 3 listeners (Task 5), idempotent recording (Task 2+4), 5-event mapping (Task 3), `/me` + audit + mark-read API (Task 7), docker-compose/build-all/gateway/privilege wiring (Task 8). "Out of scope" items (email/SMS/push, DLQ, full 17-type mapping, WebSocket) appear nowhere in any task — confirmed by re-reading the task list once more after writing it.

**Placeholder scan:** no TBD/TODO; every step has runnable code or an exact shell command.

**Type consistency:** `RecordNotificationUseCase.record(String eventId, String eventType, String aggregateId, Instant occurredAt, String rawPayload, DomainEvent typedEventOrNull)` is defined once in Task 4 and called with that exact signature in Task 5's `EventRecordingService`. `NotificationRepositoryPort` methods (`record`, `update`, `findByRecipientUserId`, `findAllFiltered`, `findById`) are defined once in Task 2 and used with matching names/arguments in Tasks 4 and 7 — no renaming drift.

**Review Focus coverage:** all 5 items listed in the header have a concrete test in the task that owns the code (cross-referenced above in that section).
