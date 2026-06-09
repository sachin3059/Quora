# Quora-like Q&A Platform — Backend

> A production-grade, reactive backend for a Q&A platform built with Java, Spring Boot 3, and WebFlux. Designed to demonstrate real-world backend engineering patterns: event-driven architecture, reactive non-blocking I/O, distributed caching, and scalable feed generation — targeting backend roles at top-tier companies.

---

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Modules](#modules)
- [Feed Generation System](#feed-generation-system)
- [Event-Driven Architecture](#event-driven-architecture)
- [Security](#security)
- [API Documentation](#api-documentation)
- [Infrastructure](#infrastructure)
- [Running Locally](#running-locally)
- [Testing](#testing)
- [Project Roadmap](#project-roadmap)
- [Key Engineering Decisions](#key-engineering-decisions)

---

## Overview

This project is a fully functional backend for a Quora-like Q&A platform. Users can register, post questions, write answers, comment, vote, follow each other, and receive a personalized feed. The system is designed around production-grade engineering principles rather than just getting things to work.

**Key highlights:**

- Fully reactive, non-blocking stack using Spring WebFlux and Reactive MongoDB
- Event-driven communication between modules via Apache Kafka
- Redis-based personalized feed with fanout-on-write architecture
- Concurrent-safe vote counting using MongoDB atomic operations
- Kafka-driven reputation system and real-time notifications
- Nginx load balancer with multiple application instances via Docker Compose
- JWT-based authentication with role-based access control
- Swagger / OpenAPI documentation for all REST endpoints

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3, Spring WebFlux |
| Database | MongoDB (Reactive) |
| Cache | Redis |
| Messaging | Apache Kafka |
| Security | Spring Security, JWT |
| Load Balancer | Nginx |
| Build Tool | Gradle |
| Testing | JUnit 5, Mockito, StepVerifier |
| Documentation | Swagger / SpringDoc OpenAPI |
| Infrastructure | Docker Compose |
| Utilities | Lombok |

---

## Architecture

The project follows a clean layered architecture with strict module boundary discipline:

```
Controller → Service Interface → Service Implementation → Repository → Mapper → DTO
```

Services are split by responsibility following the **Interface Segregation Principle**. For example, the User module exposes `UserRegistrationService`, `UserAuthService`, and `UserQueryService` as separate interfaces rather than a single bloated service.

Cross-module communication happens exclusively through **Kafka events** — services never inject repositories from other modules.

```
┌─────────────────────────────────────────────────────────┐
│                        Nginx                             │
│                   (Load Balancer)                        │
└────────────────────┬────────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          │   App Instance 1    │   App Instance 2 ...
          │   Spring WebFlux    │
          └──────────┬──────────┘
                     │
     ┌───────────────┼───────────────┐
     │               │               │
  MongoDB          Redis           Kafka
(Reactive)    (Feed / Cache)   (Event Bus)
```

---

## Modules

### User
- Registration with hashed passwords (BCrypt)
- Login with JWT token generation
- Role-based access control: `USER`, `MODERATOR`, `ADMIN`
- Profile management
- DiceBear avatar integration for auto-generated profile pictures

### Questions
- Full CRUD operations
- Tag-based categorization for feed targeting
- MongoDB text index for keyword search
- Author reputation reflected on question display

### Answers
- Post answers to questions
- Answer count tracked atomically on the parent Question document
- Kafka event published on answer creation for feed scoring updates

### Comments
- Nested comment support with `parentId`, `parentType`, and `rootId` fields
- Supports comments on both Questions and Answers
- Recursive nesting resolved at the service layer

### Votes
- Upvote / downvote on Questions and Answers
- Concurrent-safe using MongoDB `$inc` atomic operator — no read-modify-write race conditions
- Duplicate votes prevented via unique compound index `(userId, targetId, targetType)`
- Vote events published to Kafka for reputation and feed score updates

### Follow
- Follow / unfollow users
- Follower and following counts updated atomically using `$inc`
- Follow graph used as a candidate source in feed generation

### Notifications
- Kafka consumer listening across all event topics
- Events: question posted, answer received, comment added, vote cast, new follower
- Unread notification count exposed via API

### Reputation
- Kafka consumer on vote events
- Author reputation score incremented / decremented on upvote / downvote
- Score stored on User document, surfaced in API responses

### Feed
- Multi-source candidate generation (see [Feed Generation System](#feed-generation-system))
- Redis sorted set-based fanout-on-write architecture
- MongoDB fallback for cold-start users with no feed data

---

## Feed Generation System

The feed system is one of the most architecturally interesting parts of this project. It is designed in phases, inspired by LinkedIn and Twitter engineering research.

### Current Implementation — Pull + Fanout Hybrid

**Fanout on Write (Redis):**

When a user posts a question, a `QUESTION_POSTED` Kafka event triggers the `FanoutConsumer`, which writes the question ID into each follower's Redis sorted set (feed inbox) with a computed score.

```
Score = (upvotes × 3) + (answerCount × 2) + (commentCount × 1) + recencyBoost
```

This formula is inspired by the HackerNews ranking algorithm. Each weight is independently tunable.

**Cold-Start Fallback (MongoDB):**

New users with no Redis feed data fall back to a pull-based pipeline that generates a feed on demand from four parallel candidate sources:

| Source | Description |
|---|---|
| Latest | All questions sorted by creation time |
| Tag-Based | Questions matching the user's declared interest tags |
| Following | Questions from users the current user follows |
| Trending | Questions sorted by upvote count in a recent time window |

Candidates are merged via `Flux.merge()`, deduplicated by question ID, scored, sorted, and paginated.

**Vote Score Updates:**

`FeedScoreConsumer` listens to vote events and updates the score of the relevant question in all affected Redis sorted sets — keeping the feed score fresh without recomputation.

### Feed API

```
GET /api/feed?type=personalized&page=0&size=10
GET /api/feed?type=latest&page=0&size=10
GET /api/feed?type=trending&page=0&size=10
GET /api/feed?type=following&page=0&size=10
GET /api/feed?type=tags&page=0&size=10
```

---

## Event-Driven Architecture

All cross-module side effects are handled by Kafka consumers. No module directly calls another module's service or repository.

| Kafka Topic | Published By | Consumed By |
|---|---|---|
| `quora.question.posted` | Question Service | FanoutConsumer, Notification Service |
| `quora.answer.posted` | Answer Service | Notification Service, Feed Score Consumer |
| `quora.comment.posted` | Comment Service | Notification Service |
| `quora.vote.cast` | Vote Service | Reputation Consumer, Feed Score Consumer |
| `quora.user.followed` | Follow Service | Notification Service |

This design means adding a new downstream effect (e.g., sending an email) requires only a new consumer — zero changes to existing services.

---

## Security

- JWT-based stateless authentication
- Tokens signed with a secret key, validated on every request via `JwtAuthenticationFilter`
- Role-based authorization: `USER`, `MODERATOR`, `ADMIN`
- Password hashing with BCrypt
- Route-level protection via Spring Security configuration

### Exception Hierarchy

Custom exceptions map cleanly to HTTP status codes:

| Exception | HTTP Status |
|---|---|
| `ResourceNotFoundException` | 404 Not Found |
| `DuplicateResourceException` | 409 Conflict |
| `UnauthorizedException` | 403 Forbidden |
| `ValidationException` | 422 Unprocessable Entity |

---

## API Documentation

Swagger UI is available at:

```
http://localhost:8080/swagger-ui.html
```

All endpoints are documented with request/response schemas, authentication requirements, and example payloads via SpringDoc OpenAPI.

---

## Infrastructure

All infrastructure is managed via Docker Compose.

| Service | Purpose |
|---|---|
| MongoDB | Primary data store |
| Redis | Feed sorted sets, caching |
| Kafka + Zookeeper | Event bus between modules |
| Nginx | Load balancer across application instances |

### Starting Infrastructure

```bash
docker-compose up -d
```

This starts MongoDB, Redis, Kafka, Zookeeper, and Nginx. The application connects to all services on startup.

---

## Running Locally

### Prerequisites

- Java 17+
- Gradle 8+
- Docker and Docker Compose

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/your-username/quora-backend.git
cd quora-backend

# 2. Start all infrastructure
docker-compose up -d

# 3. Build and run the application
./gradlew bootRun
```

The application starts on `http://localhost:8080`.

### Environment Configuration

Key properties in `application.yml`:

```yaml
spring:
  data:
    mongodb:
      uri: mongodb://localhost:27017/quora
  redis:
    host: localhost
    port: 6379
  kafka:
    bootstrap-servers: localhost:9092

jwt:
  secret: your-secret-key
  expiration: 86400000   # 24 hours
```

---

## Testing

Tests are written using JUnit 5, Mockito, and Reactor's `StepVerifier` for reactive stream assertions.

```bash
# Run all tests
./gradlew test
```

`StepVerifier` is used across service tests to assert on reactive pipelines:

```java
StepVerifier.create(questionService.getById(questionId))
    .expectNextMatches(q -> q.getTitle().equals("Expected Title"))
    .verifyComplete();
```

---

## Project Roadmap

| Phase | Status | Description |
|---|---|---|
| Phase 1 — Core Backend | ✅ Done | Users, Questions, Answers, Comments, Votes, Follow |
| Phase 1 — Events | ✅ Done | Kafka-driven Notifications and Reputation |
| Phase 1 — Feed | ✅ Done | Redis fanout feed with MongoDB fallback |
| Phase 1 — Security | ✅ Done | JWT auth, RBAC, custom exception hierarchy |
| Phase 1 — Infra | ✅ Done | Docker Compose, Nginx load balancer |
| Phase 1 — Docs | ✅ Done | Swagger / OpenAPI documentation |
| Phase 2 — Caching | 🔜 Planned | Redis caching for profiles, trending feed; Kafka-driven invalidation |
| Phase 2 — Rate Limiting | 🔜 Planned | Redis sliding window counter via WebFilter |
| Phase 2 — Auth | 🔜 Planned | Refresh token support |
| Phase 3 — Pagination | 🔜 Planned | Cursor-based pagination across all modules |
| Phase 3 — Soft Delete | 🔜 Planned | `isDeleted` flag for moderation and audit trail |
| Phase 4 — ML Ranking | 🔮 Future | LightGBM model trained on interaction logs via Kafka |
| Phase 5 — LLM Features | 🔮 Future | Semantic search, answer quality scoring, auto-tagging |

---

## Key Engineering Decisions

**Why Spring WebFlux over Spring MVC?**
Non-blocking I/O handles high concurrency with fewer threads. Feed generation involves parallel database calls — `Flux.merge()` runs all four candidate sources simultaneously, which would require thread pools in a blocking model.

**Why Kafka for cross-module events?**
Decouples modules completely. The Vote service does not know about Reputation or Feed — it just publishes an event. New consumers can be added without touching existing code.

**Why Redis sorted sets for the feed?**
O(log N) insertion and O(log N + M) range queries make sorted sets ideal for scored feed inboxes. Fanout-on-write means feed reads are O(M) — just a range query — rather than a full recomputation.

**Why atomic `$inc` for counters?**
Read-modify-write patterns create race conditions under concurrent votes. MongoDB's `$inc` operator applies the increment atomically at the database level, eliminating this class of bug entirely.

**Why module boundary discipline?**
Injecting `AnswerRepository` into `CommentService` couples modules at the data layer. If the Answer schema changes, Comment breaks. All cross-module data needs are resolved at the Kafka consumer layer instead.

**Why Nginx in Docker Compose?**
Demonstrates awareness of the full deployment stack. Nginx sits in front of multiple application instances and handles load distribution, TLS termination, and request routing — closer to how this service would actually run in production.

---

> **Note:** This is a portfolio project demonstrating backend engineering depth. The architecture is deliberately designed so each phase is independently deployable — the rule-based feed scorer can be swapped for an ML model without changing candidate generation or delivery layers.

## Personalized Feed Generation - References

The following engineering articles are useful for understanding how large-scale platforms generate and rank personalized feeds:

### LinkedIn Engineering

1. Engineering the Next Generation of LinkedIn's Feed  
   https://www.linkedin.com/blog/engineering/feed/engineering-the-next-generation-of-linkedins-feed

2. Understanding Feed Dwell Time  
   https://www.linkedin.com/blog/engineering/feed/understanding-feed-dwell-time

3. Community-Focused Feed Optimization  
   https://www.linkedin.com/blog/engineering/feed/community-focused-feed-optimization

4. Making Your Feed More Relevant (Part I)  
   https://www.linkedin.com/blog/engineering/archive/making-your-feed-more-relevant-part-i

## Key Concepts Covered

- Candidate Generation
- Feed Ranking
- Personalization
- User Engagement Signals
- Dwell Time
- Recommendation Systems
- Feed Relevance Optimization
- Large-Scale Feed Architecture