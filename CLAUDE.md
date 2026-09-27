# Project Nexus — Polyrepo conventions (for Claude)

This file exists so implementing a new feature doesn't require re-reading the whole
codebase from scratch. It captures conventions already established across
`user-service` and `catalog-service` that every new service should follow. **It is a
shortcut, not ground truth** — if something here looks inconsistent with the actual code
you're editing, trust the code and update this file.

## Repo layout

Polyrepo: each microservice is its own GitHub repo under `antran19`, cloned as a sibling
under one parent folder (this one, `C:\FPT`, on this machine):

```
common-libs/       # shared libraries (see below)
discovery-server/  # Eureka, port 8761, no dependencies
api-gateway/       # Spring Cloud Gateway (WebFlux/reactive, NOT servlet), port 8080
user-service/      # port 8081
catalog-service/   # port 8082
auction-service/   # port 8083 (next new port for any further service: 8084, ...)
infra/             # docker-compose for the whole cluster; docs/superpowers/specs and
                    # docs/superpowers/plans live here too, until a service repo exists
                    # to own them (see infra/docs/HANDOFF.md for full project status)
```

There is also a separate, unrelated **solo repo** (`project-nexus`, a Maven monorepo) —
do not confuse the two. The polyrepo above is the active one for the team submission.

Original SRS ("Project Nexus SRS v1.0") is a `.docx` **not tracked in any repo** — ask the
user for its current path if you need to re-derive requirements for a service not yet
speced.

## Architecture (every service follows this identically)

Hexagonal, one Maven module per service, no parent reactor:

```
src/main/java/com/nexus/<service>/
├── api/            # Controllers, request/response DTOs, MapStruct mappers
├── application/
│   ├── usecase/    # One class per use case, plain constructor injection, no Spring
│   │                 annotations on the use case class itself
│   ├── port/out/   # Interfaces the use cases depend on (RepositoryPort, EventPublisherPort)
│   └── exception/  # Extend common-core's DomainException subclasses
├── domain/
│   ├── model/      # Plain Java, zero framework imports
│   └── service/    # Stateless domain policies (validation/business rules), zero framework imports
└── infrastructure/
    ├── persistence/   # JPA entities (suffix JpaEntity) + adapters implementing the ports
    ├── messaging/     # OutboxEventPublisherAdapter + OutboxRelayJob (see below)
    └── config/        # SecurityConfig, UseCaseConfig (see below)
```

One Postgres DB per service, Flyway-migrated (`src/main/resources/db/migration/V1__*.sql`).
Registers with Eureka. Routes through `api-gateway`.

## common-libs (4 modules — read these directly if unsure, they're small)

- **`common-core`**: `ApiResponse<T>`/`ApiError`/`FieldError` (standard response envelope),
  exception hierarchy under `com.nexus.common.core.exception`: `DomainException` (base),
  `NotFoundException`, `ConflictException`, `ForbiddenException`, `UnauthorizedException`,
  `ValidationException` — each takes `(errorCode, message)`. Throw these from use cases;
  never build `ResponseEntity` error responses by hand.
- **`common-web`**: `GlobalExceptionHandler` (`@RestControllerAdvice`) maps all of the
  above to HTTP status + `ApiResponse.error(...)` automatically. Reuse as-is, never
  reimplement per-service.
- **`common-security`**: `@RequiresPrivilege("X.Y")` (method-level annotation, enforced by
  `PrivilegeAuthorizationAspect` against the JWT's `privileges` claim — a `List<String>`).
  `JwtAuthenticationFilter` sets `SecurityContextHolder`'s principal to the JWT subject
  (the caller's user id, as a `String`) with `SimpleGrantedAuthority` per privilege string.
  Note: this filter is `@ConditionalOnClass(name = "jakarta.servlet.Filter")` — it silently
  does not load in `api-gateway` (WebFlux, no servlet API on the classpath); don't expect
  `@RequiresPrivilege` semantics there, only coarse JWT signature/expiry checking.
- **`common-events`**: `DomainEvent` abstract base (`eventId`, `eventType`, `occurredAt`,
  `aggregateId` — constructor takes `(eventType, aggregateId)`). Every new domain event
  extends this. Adding events here requires bumping `common-libs`'s version and publishing
  (`git push` to `common-libs` main auto-publishes to GitHub Packages via CI) before a
  dependent service can consume the new version.

## Security convention (every `SecurityConfig`)

```java
.authorizeHttpRequests(auth -> auth
        .requestMatchers(HttpMethod.GET, "/api/v1/**").permitAll()
        .requestMatchers("/actuator/**").permitAll()
        .anyRequest().authenticated())
```

All GET is public at the gateway/Spring-Security layer; finer-grained read restrictions (if
ever needed) would need an explicit `@RequiresPrivilege` — none of the current read
endpoints use one. Every write endpoint requires a valid JWT (`anyRequest().authenticated()`)
plus, inside the controller/use case, `@RequiresPrivilege` for the specific action.

Cross-service IDs (`sellerId`, `productId`, `bidderId`, etc.) are **always opaque UUID
strings** — never resolved via a synchronous call to the owning service. Services trust the
JWT and the caller-supplied ID; there is no service-to-service REST client anywhere in this
codebase yet.

## Outbox pattern (every service that publishes events)

```java
@Component
public class OutboxRelayJob {
    @Scheduled(fixedDelay = 5000)
    public synchronized void relayPendingEvents() {
        List<OutboxJpaEntity> pending = outboxJpaRepository.findTop50ByPublishedAtIsNullOrderByCreatedAtAsc();
        // kafkaTemplate.send(TOPIC, row.getAggregateId(), row.getPayload()).get(5, TimeUnit.SECONDS);
        // on success: row.markPublished(Instant.now()); save; on failure: log, leave unpublished for next poll
    }
}
```

Topic name convention: `<service>-events` (`catalog-events`, will be `auction-events`).
Use case writes the domain event row to the `outbox` table in the *same transaction* as
the business write — never publish to Kafka directly from a use case.

## UseCaseConfig convention

Every use case is a plain class (no `@Service`), wired as a `@Bean` in
`infrastructure/config/UseCaseConfig.java`, constructor-injected with its ports. Keeps
`domain`/`application` free of Spring imports.

## Tech stack (pin exactly, don't drift between services)

Java 21 · Spring Boot 3.3.4 · Spring Cloud 2023.0.3 · Spring Data JPA + Postgres 16 +
Flyway · Spring Kafka · Spring Security (stateless JWT) · MapStruct 1.6.2 · JUnit 5 +
AssertJ + Mockito + Testcontainers 1.20.1 (Postgres, Kafka).

## Testing convention

Unit tests for `domain/service` policies with **no Spring context** (plain JUnit).
Testcontainers-backed integration tests for repository adapters and the outbox write
(real Postgres, not mocked — FTS/locking/constraint correctness can't be verified with
mocks). `GlobalExceptionHandlerTest` is reused unmodified across services, not rewritten.

## Building/running a service standalone

```bash
mvn clean package     # runs full test suite, Testcontainers needs Docker running
java -jar target/<service>-0.1.0-SNAPSHOT.jar   # needs discovery-server + Postgres up
```

Full cluster: see `infra/README.md` (`./build-all.sh && docker compose build && docker
compose up`, siblings-cloned layout required).

## Where design docs live

`docs/superpowers/specs/YYYY-MM-DD-<service>-design.md` and
`docs/superpowers/plans/YYYY-MM-DD-<service>.md` — currently all inside the `infra` repo
(since most service repos don't pre-exist before their spec is written); may move into a
service's own repo once one exists, check both locations.
