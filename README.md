# infra

Local docker-compose setup that runs Project Nexus's full microservice cluster
(polyrepo team submission). No single service repo owns "the whole system", so this repo
holds that: Postgres (x2), Kafka, and all 4 application services wired together.

See [`docs/adr/0001-polyrepo-for-team-submission.md`](docs/adr/0001-polyrepo-for-team-submission.md)
for why this is a separate repo instead of a monorepo.

## Expected layout

Clone every repo as a sibling of this one:

```
FPT/
  common-libs/
  discovery-server/
  api-gateway/
  user-service/
  catalog-service/
  infra/          <- this repo
```

## Run everything locally

```bash
./build-all.sh          # mvn install common-libs, mvn package the other 4
docker compose build
docker compose up
```

`build-all.sh` builds every jar first because each service's `Dockerfile` just copies a
pre-built `target/*.jar` — no Maven build happens inside Docker, so no GitHub Packages
token needs to reach the image build.

Once up: Eureka dashboard at `http://localhost:8761`, gateway at `http://localhost:8080`,
`user-service` directly at `:8081`, `catalog-service` directly at `:8082`.

## Follow-up (not done yet)

- Point each service's `Dockerfile`/this compose file at pre-built images from a registry
  (GHCR) instead of always building locally, once each repo's CI publishes one.
- A smoke-test script exercising the full register → login → category → product → search
  flow through the gateway (the original monorepo has one at
  `infra/scripts/smoke-test.sh` worth adapting).
