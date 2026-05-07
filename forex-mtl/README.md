# Forex Proxy — Paidy Take-Home

A local proxy service for currency exchange rates, built on top of the One-Frame API.

## Problem

The upstream One-Frame service allows a maximum of **1,000 requests/day** per token. The proxy must support **10,000+ requests/day**.

## Solution

**Batch fetch + in-memory cache with TTL.**

- On the first request (or after the cache expires), the proxy fetches **all 72 currency pairs** in a single HTTP call to One-Frame.
- All subsequent requests are served from the in-memory cache.
- The cache expires after **5 minutes** (per the requirement that rates must not be older than 5 minutes).

**The math:**
- 9 supported currencies → 72 pairs (9 × 8, excluding same-currency)
- One upstream call every 5 minutes = **288 calls/day** — well under the 1,000 limit
- 10,000+ requests/day all served from cache ✅

**Thundering herd protection:**  
A `Ref[F, Option[Deferred[F, ...]]]` ensures only one fiber refreshes the cache at a time. All other concurrent requests wait on the same `Deferred` and unblock simultaneously when the refresh completes — O(1) fan-out, not O(n) sequential.

## Supported Currencies

`AUD`, `CAD`, `CHF`, `EUR`, `GBP`, `NZD`, `JPY`, `SGD`, `USD`

## Running the Service

### Prerequisites
- Java 17
- sbt
- Docker (for One-Frame)

### 1. Start One-Frame

One-Frame's Docker image exposes port 8080 internally. Map it to 8081 to avoid conflicting with the proxy which also listens on 8080:

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

### 3. Query rates

```bash
curl "http://localhost:8080/rates?from=USD&to=JPY"
```

Response:
```json
{
  "from": "USD",
  "to": "JPY",
  "price": 0.8963426128853093,
  "timestamp": "2026-05-07T13:12:22Z"
}
```

## Running Tests

```bash
cd forex-mtl
ONE_FRAME_TOKEN=test sbt test
```

## Configuration

`src/main/resources/application.conf` (with environment variable overrides):

```hocon
app {
  http {
    host = "0.0.0.0"          # override: HTTP_HOST
    port = 8080               # override: HTTP_PORT
    timeout = 40 seconds
  }
  one-frame {
    host = "localhost"        # override: ONE_FRAME_HOST
    port = 8081               # override: ONE_FRAME_PORT
    token = ${ONE_FRAME_TOKEN}  # required — no default
    cache-ttl-minutes = 5     # MUST be <= 5 per Forex.md requirement
    cb-max-failures = 3       # consecutive failures before circuit opens
    cb-reset-timeout-seconds = 30
  }
}
```

## Error Responses

| Scenario | HTTP Status | Response |
|---|---|---|
| Valid pair | 200 | JSON rate object |
| Invalid currency code | 400 | `Unsupported currency: XYZ` |
| Same currency (`from == to`) | 400 | `Cannot convert a currency to itself` |
| Missing `from` param | 400 | `Query parameter 'from' is required` |
| Missing `to` param | 400 | `Query parameter 'to' is required` |
| One-Frame unavailable / CB open | 503 | `Exchange rate service is temporarily unavailable. Please try again shortly.` |

## Design Decisions

- **`RateFetcher` trait**: Decouples `CachingInterpreter` from `OneFrameClient`, making the cache testable without real HTTP calls.
- **`Ref` + `Deferred` for refresh coordination**: `Ref[F, Option[Deferred[F, ...]]]` replaces a `Semaphore`. The first fiber to see a stale cache installs a `Deferred` and starts the fetch; all others `.get` the same `Deferred` and unblock together when it completes.
- **Circuit breaker with HalfOpen**: After `cbMaxFailures` consecutive failures the circuit opens. After `cbResetTimeout`, one probe request is allowed. Probe success → Closed; probe failure → Open with a fresh timer.
- **Retries inside circuit breaker**: `retryN` handles transient blips; the CB handles sustained outages. Putting retries inside the CB is correct — it prevents the CB from false-tripping on a single transient error.
- **`Currency.fromString` returns `Either`**: Validates input at the boundary; returns a descriptive 400 instead of 500 on unknown currencies.
- **All pairs fetched at once**: One-Frame supports batch pair queries. Fetching all 72 pairs in one call avoids per-pair quota exhaustion.

## Assumptions

- One-Frame is available locally on the configured host/port.
- The 5-minute TTL is measured from the time the fetch completed, not from the `time_stamp` field in the One-Frame response (see Q6 in INTERVIEW_NOTES.md for rationale).
- Same-currency pairs (`USD→USD`) are not valid requests and return a 400.
- This service is designed for a single instance. Horizontal scaling requires a shared distributed cache (e.g. Redis) and a distributed lock to prevent multiple instances from exhausting the quota simultaneously.
- **Known limitation:** If One-Frame returns a partial response (fewer than 72 pairs), the incomplete set is cached for the full TTL. Requests for missing pairs will receive a 503 until the cache expires. Validating response completeness was intentionally avoided — it would couple the code to a specific currency count and require a patch whenever One-Frame adds new pairs.
