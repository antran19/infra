# ADR-0001: Polyrepo, not monorepo, for the team submission

**Status:** Accepted — 2026-09-25

## Context

The professor requires each microservice to be its own git repository and its own
independent Spring Boot project. The leader's separate personal project ("Project Nexus")
uses a monorepo (see its own ADR-0002) — that choice doesn't apply here; it was made for a
solo-developer context and this is a graded team submission with an explicit polyrepo
requirement.

## Decision

Each service is its own repository, buildable independently (own `pom.xml`, no shared
Maven reactor):

- [`common-libs`](https://github.com/antran19/common-libs) — shared code
  (`common-core`, `common-web`, `common-events`, `common-security`), published as
  versioned Maven artifacts via GitHub Packages
- [`discovery-server`](https://github.com/antran19/discovery-server) — Eureka registry
- [`api-gateway`](https://github.com/antran19/api-gateway) — Spring Cloud Gateway
- [`user-service`](https://github.com/antran19/user-service)
- [`catalog-service`](https://github.com/antran19/catalog-service)
- `infra` (this repo) — docker-compose to run the whole cluster locally, since no single
  service repo owns "the system"

## Consequences

- Cross-cutting changes (editing shared code in `common-libs`) now require bumping its
  version and updating each consumer's `pom.xml`, instead of a single reactor rebuild.
- Every service repo that depends on `common-libs` needs GitHub Packages read access
  configured (a token in `~/.m2/settings.xml` locally, `packages: read` + the built-in
  `GITHUB_TOKEN` in CI) — documented in each repo's own README.
- Local development and demos need all repos cloned as siblings and run through this
  `infra` repo's `docker-compose.yml`, since no repo can build a runnable multi-service
  stack on its own.
