# Forex Rate Proxy

A local HTTP proxy for currency exchange rates. Internal services call this proxy instead of calling One-Frame directly. It centralises caching, resilience, and request validation.

## Requirements (From the Brief)

1. Return an exchange rate for two supported currencies.
2. Returned rate must never be older than **5 minutes**.
3. Support **at least 10,000 successful requests/day** with an API token capped at **1,000 upstream calls/day**.

## Approach

The rate-limit mismatch rules out per-request forwarding. The only viable approach is to decouple client request volume from upstream call volume entirely — fetch all rates in bulk and serve every client request from an in-memory cache.

With 9 supported currencies there are 72 directional pairs (9 × 8, excluding same-currency). One-Frame supports batch queries, so a single upstream call fetches all 72 at once. With a 5-minute TTL that is at most **288 upstream calls/day** — well within the 1,000 limit — while the proxy can serve an arbitrary number of client requests from memory.

### Rate Limit Math

| | Value |
|---|---|
| Supported currencies | 9 |
| Directional pairs (9 × 8) | 72 |
| Cache TTL | 5 minutes |
| Upstream calls per hour | 12 |
| **Upstream calls per day** | **288** |
| One-Frame daily limit | 1,000 |
| Headroom | 3.4× |

### Thundering Herd

When the cache expires, multiple concurrent requests will all observe a stale cache simultaneously and each attempt a refresh. Naively, this defeats the quota savings — N requests would fire N upstream calls.

The solution is a `Ref[F, Option[Deferred[F, ...]]]`. When the cache is stale, fibers race to install a `Deferred` via compare-and-swap. Exactly one wins; it owns the refresh and completes the `Deferred` when done. All other fibers see the already-installed `Deferred` and block on it, then unblock simultaneously when the winner completes — one upstream call regardless of concurrency.

### Resilience

A caching layer that silently hammers a degraded upstream is worse than useless. To guard against this, the proxy uses a circuit breaker — a component that watches how many consecutive upstream calls have failed and temporarily stops making new ones if the count crosses a threshold (`cbMaxFailures`). During this open period, requests fail fast with a 503 rather than waiting for a connection that is unlikely to succeed.

To recover gracefully, the circuit does not flip straight back to healthy. After a configurable cooldown (`cbResetTimeout`), it allows exactly one test request through — a probe. If the probe succeeds, the circuit closes and normal traffic resumes. If it fails, the cooldown restarts. This three-state model (closed → open → half-open → closed) avoids both premature recovery and indefinite unavailability.

Retries for short-lived blips are handled separately, inside the circuit breaker. A single transient failure is retried before being counted — so one bad response does not immediately move the failure counter.

### Testability

The caching layer and the HTTP client are kept deliberately separate. The caching component — which owns cache hit/miss logic, thundering herd protection, retries, and circuit breaker decisions — talks to an interface (`RateFetcher`) rather than directly to the HTTP client. In tests, this interface is replaced with a simple stub that returns whatever the test needs. This means all the complex coordination logic can be exercised without spinning up a real HTTP server or Docker container. The HTTP client is tested independently against its own concerns.

## API

```
GET /rates?from={CURRENCY}&to={CURRENCY}
```

Supported currencies: `AUD CAD CHF EUR GBP JPY NZD SGD USD`

### Success (200)

```json
{
  "from": "USD",
  "to": "JPY",
  "price": 0.8963426128853093,
  "timestamp": "2026-05-07T13:12:22Z"
}
```

### Error Responses

| Status | When | Body |
|---|---|---|
| 400 | Invalid or unsupported currency | `Unsupported currency: XYZ` |
| 400 | Same currency for both params | `Cannot convert a currency to itself` |
| 400 | Missing `from` parameter | `Query parameter 'from' is required` |
| 400 | Missing `to` parameter | `Query parameter 'to' is required` |
| 503 | One-Frame unavailable / circuit open | `Exchange rate service is temporarily unavailable. Please try again shortly.` |

## Running Locally

### Prerequisites
- Java 17
- sbt
- Docker (for One-Frame)

### 1. Start One-Frame

Map its internal port 8080 to 8081 to avoid conflicting with the proxy:

```bash
docker pull paidyinc/one-frame
docker run -p 8081:8080 paidyinc/one-frame
```

### 2. Start the proxy

`ONE_FRAME_TOKEN` is required — the service will not start without it:

```bash
cd forex-mtl
ONE_FRAME_TOKEN=10dc303535874aeccc86a8251e6992f5 sbt run
```

The proxy starts on `http://localhost:8080`.

### 3. Quick check

```bash
curl "http://localhost:8080/rates?from=USD&to=JPY"
curl "http://localhost:8080/rates?from=USD&to=USD"   # 400
curl "http://localhost:8080/rates?from=BTC&to=USD"   # 400
```

## Tests

```bash
cd forex-mtl
ONE_FRAME_TOKEN=test sbt test
```

The suite runs entirely in-memory — no network or Docker required.

| Spec | What it covers |
|---|---|
| `RatesHttpRoutesSpec` | Currency parsing, case sensitivity, all supported values |
| `RatesHttpRoutesIntegrationSpec` | Full HTTP layer — 200, 400 variants, 503 |
| `CircuitBreakerSpec` | Closed/Open/HalfOpen transitions, probe behaviour, concurrent HalfOpen rejection |
| `CachingInterpreterSpec` | Cache hit/miss, TTL expiry, retries, thundering herd coalescing |

## Configuration

`src/main/resources/application.conf` (environment variable overrides in parentheses):

| Key | Default | Env override |
|---|---|---|
| `http.host` | `0.0.0.0` | `HTTP_HOST` |
| `http.port` | `8080` | `HTTP_PORT` |
| `http.timeout` | `40 seconds` | — |
| `one-frame.host` | `localhost` | `ONE_FRAME_HOST` |
| `one-frame.port` | `8081` | `ONE_FRAME_PORT` |
| `one-frame.token` | *(required, no default)* | `ONE_FRAME_TOKEN` |
| `one-frame.cache-ttl-minutes` | `5` | — |
| `one-frame.cb-max-failures` | `3` | — |
| `one-frame.cb-reset-timeout-seconds` | `30` | — |

## Assumptions

- The 5-minute TTL is measured from when the fetch completed, not from the `time_stamp` field in the One-Frame response.
- Same-currency pairs (`USD→USD`) are rejected with a 400 — the spec requires two different currencies.
- One-Frame is assumed to stay within quota under normal operation (288 upstream calls/day vs a 1,000 limit). If quota is somehow exhausted, requests will return 503 until the circuit breaker resets.
- If One-Frame returns a partial response, the incomplete set is cached for the full TTL and requests for missing pairs return 503 until expiry. Validating response completeness was intentionally avoided — it would couple the code to a specific currency count and require a patch whenever currencies change.
- This service is designed for a single instance. Horizontal scaling would require a shared cache and distributed lock to prevent multiple instances from exhausting the daily quota simultaneously.

## Possible Extensions

### Request correlation (`X-Request-ID`)
Tag each request with a correlation ID — propagate if present in the incoming header, generate a UUID otherwise, and echo it back in the response. Makes request tracing straightforward when this proxy sits behind a gateway or alongside other services in a distributed system.

### Stale-while-revalidate cache
Introduce a soft TTL (e.g. 4 minutes) alongside the hard 5-minute ceiling. Requests arriving between 4–5 minutes are served immediately from the stale cache while a background fiber triggers a refresh. This eliminates the blocking window at cache expiry and reduces tail latency under steady traffic.

### Distributed cache (Redis)
The `RateFetcher` abstraction already decouples the cache from the HTTP client. Swapping the in-memory `Ref` for a Redis-backed implementation would enable horizontal scaling without each instance exhausting quota independently.

### Per-IP rate limiting
Enforce a request cap per client IP to prevent a single caller from monopolising cache refreshes or saturating the service under load.
