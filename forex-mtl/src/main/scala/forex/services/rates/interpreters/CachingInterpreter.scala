package forex.services.rates.interpreters

import cats.effect.{ Clock, Concurrent, Sync, Timer }
import cats.effect.concurrent.{ Deferred, Ref }
import cats.implicits._
import forex.config.OneFrameConfig
import forex.domain.Rate
import forex.services.rates.Algebra
import forex.services.rates.errors.Error
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

private[interpreters] case class CacheEntry(
    rates: Map[Rate.Pair, Rate],
    fetchedAt: Long
)

class CachingInterpreter[F[_]: Concurrent: Clock: Timer](
    fetcher: RateFetcher[F],
    ttlMillis: Long,
    cacheRef: Ref[F, Option[CacheEntry]],
    inFlightRef: Ref[F, Option[Deferred[F, Either[Error, CacheEntry]]]],
    circuitBreaker: CircuitBreaker[F]
) extends Algebra[F] {

  private val log = LoggerFactory.getLogger(getClass)

  override def get(pair: Rate.Pair): F[Error Either Rate] =
    for {
      now <- Clock[F].realTime(TimeUnit.MILLISECONDS)
      entry <- cacheRef.get
      result <- entry match {
                 // Cache is fresh — serve directly, no synchronisation needed
                 case Some(cached) if now - cached.fetchedAt < ttlMillis =>
                   Sync[F].delay(log.debug(s"Cache hit for $pair (age: ${now - cached.fetchedAt}ms)")) *>
                     (cached.rates.get(pair).toRight(Error.RateServiceUnavailable): Either[Error, Rate])
                       .pure[F]

                 // Cache is stale or empty — join an in-flight refresh or start one
                 case _ =>
                   Sync[F].delay(log.info(s"Cache stale or empty — triggering refresh for $pair")) *>
                     getOrStartRefresh.map {
                       case Right(e) =>
                         (e.rates.get(pair).toRight(Error.RateServiceUnavailable): Either[Error, Rate])
                       case Left(err) =>
                         Left(err): Either[Error, Rate]
                     }
               }
    } yield result

  // one fiber does the fetch; others wait on the same Deferred
  private def getOrStartRefresh: F[Either[Error, CacheEntry]] =
    inFlightRef.get.flatMap {
      case Some(existing) =>
        // already running, wait
        existing.get
      case None =>
        // might be first — try to install
        Deferred[F, Either[Error, CacheEntry]].flatMap { fresh =>
          inFlightRef.modify {
            case Some(existing) => (Some(existing), existing.get) // lost the race, wait
            case None           => (Some(fresh), doRefresh(fresh)) // won the race, fetch
          }.flatten
        }
    }

  private def doRefresh(deferred: Deferred[F, Either[Error, CacheEntry]]): F[Either[Error, CacheEntry]] = {
    val fetch: F[Either[Error, CacheEntry]] =
      circuitBreaker
        .protect(retryN(3, 100L)(fetcher.fetchAll()))
        .flatMap {
          case Right(ratesMap) =>
            Clock[F].realTime(TimeUnit.MILLISECONDS).flatMap { fetchedAt =>
              val entry = CacheEntry(ratesMap, fetchedAt)
              cacheRef.set(Some(entry)).as(Right(entry): Either[Error, CacheEntry])
            }
          case Left(err) =>
            Concurrent[F].pure(Left(err): Either[Error, CacheEntry])
        }
        .attempt
        .flatMap {
          case Right(result) => Concurrent[F].pure(result)
          case Left(e) =>
            Sync[F]
              .delay(log.error(s"Unexpected error during cache refresh: ${Option(e.getMessage).getOrElse(e.toString)}"))
              .as(Left(Error.RateServiceUnavailable): Either[Error, CacheEntry])
        }

    Concurrent[F].guarantee(
      fetch.flatTap { result =>
        val logEffect = result match {
          case Right(_)  => Sync[F].delay(log.info("Cache refreshed successfully"))
          case Left(err) => Sync[F].delay(log.error(s"Cache refresh failed: $err"))
        }
        logEffect *> deferred.complete(result)
      }
    )(
      // clear slot and unblock any waiters on cancellation
      inFlightRef.set(None) *>
        deferred.complete(Left(Error.RateServiceUnavailable)).attempt.void
    )
  }

  // Retries action up to `attempts` times with exponential backoff.
  private def retryN[A](attempts: Int, delayMs: Long)(action: F[Either[Error, A]]): F[Either[Error, A]] =
    action.flatMap {
      case right @ Right(_) => Concurrent[F].pure(right)
      case Left(_) if attempts > 1 =>
        Sync[F].delay(log.warn(s"Upstream call failed, retrying in ${delayMs}ms (attempts left: ${attempts - 1})")) *>
          Timer[F].sleep(delayMs.millis) *> retryN(attempts - 1, delayMs * 2)(action)
      case left @ Left(_) => Concurrent[F].pure(left)
    }
}

object CachingInterpreter {
  // Default TTL used by tests that construct CachingInterpreter directly.
  val ttlMillis: Long = 5 * 60 * 1000L

  def apply[F[_]: Concurrent: Clock: Timer](fetcher: RateFetcher[F], config: OneFrameConfig): F[Algebra[F]] =
    for {
      cacheRef <- Ref.of[F, Option[CacheEntry]](None)
      inFlightRef <- Ref.of[F, Option[Deferred[F, Either[Error, CacheEntry]]]](None)
      cb <- CircuitBreaker[F](config.cbMaxFailures, config.cbResetTimeoutMillis)
    } yield new CachingInterpreter[F](fetcher, config.cacheTtlMillis, cacheRef, inFlightRef, cb)
}
