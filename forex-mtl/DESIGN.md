# Forex Proxy — System Design

## Request Flow

```mermaid
flowchart TD
    A([GET /rates?from=USD&to=JPY]) --> B[RatesHttpRoutes]

    B --> C{Valid params?}
    C -- invalid currency --> D([400 Bad Request])
    C -- from == to --> E([400 Bad Request])
    C -- valid --> F[CachingInterpreter]

    F --> G{Cache fresh?\nage < 5 min}
    G -- yes --> H([200 OK — served from memory])
    G -- no --> I[CAS Ref — install Deferred\nfirst fiber wins, others wait on it]

    I --> J{Was a Deferred\nalready in-flight?}
    J -- yes, join existing Deferred --> H
    J -- no, this fiber owns refresh --> K[refreshCache]

    K --> L[CircuitBreaker]
    L --> M{Circuit state?}
    M -- Open --> N([503 — upstream unavailable])
    M -- Closed / HalfOpen probe --> O[retryN — up to 3 attempts]

    O --> P[OneFrameClient\nGET /rates?pair=USDJPY&pair=EURJPY&...all 72]
    P -- success --> Q[Store all 72 rates in Ref\nupdate fetchedAt timestamp\ncomplete Deferred]
    Q --> H
    P -- fail, attempts left --> R[wait 100ms / 200ms / 400ms]
    R --> O
    P -- fail, no attempts left --> S[increment failure count\nopen circuit if count >= 3\ncomplete Deferred with Left]
    S --> N
```

---

## Rate Limit Math

| | Value |
|---|---|
| Currencies supported | 9 |
| Pairs (9 × 8, no same-currency) | 72 |
| Cache TTL | 5 minutes |
| Upstream calls per hour | 12 |
| **Upstream calls per day** | **288** |
| One-Frame daily limit | 1,000 |
| Daily requests we can serve | 10,000+ |

288 upstream calls cover 10,000+ user requests because everything else is served from the in-memory cache.

---

## Concurrency Model

```mermaid
sequenceDiagram
    participant R1 as Request 1
    participant R2 as Request 2
    participant R3 as Request 3
    participant Ref as Ref[Option[Deferred]]
    participant OFC as OneFrameClient

    Note over R1,OFC: Cache is stale — 3 requests arrive at the same time

    R1->>Ref: CAS None → Some(Deferred) — wins
    R2->>Ref: CAS None → Some(Deferred) — sees Some, joins Deferred
    R3->>Ref: CAS None → Some(Deferred) — sees Some, joins Deferred

    R1->>OFC: fetchAll() — all 72 pairs
    OFC-->>R1: 72 rates
    R1->>Ref: set(None) via guarantee
    R1->>R1: complete(Deferred) — unblocks R2, R3

    Note over R1,OFC: Only 1 upstream call for 3 concurrent requests
```

---

## Circuit Breaker States

```mermaid
stateDiagram-v2
    [*] --> Closed

    Closed --> Closed : request success\n(reset failure count)
    Closed --> Closed : request failure\n(count < maxFailures)
    Closed --> Open : request failure\n(count >= maxFailures)

    Open --> Open : request arrives\nfail fast — no upstream call
    Open --> HalfOpen : resetTimeout passes

    HalfOpen --> Closed : probe request succeeds
    HalfOpen --> Open : probe request fails\n(fresh timer)
```

---

## Component Responsibilities

```mermaid
graph LR
    subgraph HTTP Layer
        Routes[RatesHttpRoutes\nValidates params\nMaps errors to status codes]
    end

    subgraph Service Layer
        Cache[CachingInterpreter\nRef — thread-safe cache\nRef+Deferred — thundering herd\nCircuitBreaker — upstream health\nretryN — transient failures]
    end

    subgraph Client Layer
        Client[OneFrameClient\nBatch fetches all 72 pairs\nSingle HTTP call per refresh]
    end

    subgraph Abstraction
        Trait[RateFetcher trait\nDecouples cache from HTTP\nEnables unit testing]
    end

    Routes --> Cache
    Cache --> Trait
    Trait -.implemented by.-> Client
```
