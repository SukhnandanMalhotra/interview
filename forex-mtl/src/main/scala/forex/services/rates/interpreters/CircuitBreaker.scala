package forex.services.rates.interpreters

import cats.effect.{ Clock, Concurrent, Sync }
import cats.effect.concurrent.Ref
import cats.implicits._
import forex.services.rates.errors.Error
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

private[interpreters] sealed trait CBState
private[interpreters] object CBState {
  final case class Closed(failures: Int) extends CBState
  case object HalfOpen extends CBState
  final case class Open(openedAt: Long) extends CBState
}

// Tracks upstream health. After maxFailures consecutive failures the circuit opens
// and all requests fail fast. After resetTimeoutMs one probe is allowed through
// (HalfOpen). Probe success → Closed(0). Probe failure → Open(now) with fresh timer.
private[interpreters] class CircuitBreaker[F[_]: Concurrent: Clock](
    maxFailures: Int,
    resetTimeoutMs: Long,
    stateRef: Ref[F, CBState]
) {
  import CBState._

  private val log = LoggerFactory.getLogger(getClass)

  def protect[A](action: F[Either[Error, A]]): F[Either[Error, A]] =
    for {
      now <- Clock[F].realTime(TimeUnit.MILLISECONDS)
      allowed <- stateRef.modify(decide(now))
      result <- if (allowed) run(action, now)
               else Concurrent[F].pure(Left(Error.RateServiceUnavailable))
    } yield result

  // Atomically decides whether to allow the request and transitions state.
  private def decide(now: Long)(state: CBState): (CBState, Boolean) = state match {
    case Open(openedAt) if now - openedAt >= resetTimeoutMs => (HalfOpen, true) // allow one probe
    case Open(_)                                            => (state, false) // still within timeout
    case HalfOpen                                           => (HalfOpen, false) // probe in-flight, reject others
    case Closed(_)                                          => (state, true) // healthy, allow
  }

  private def run[A](action: F[Either[Error, A]], now: Long): F[Either[Error, A]] =
    action.flatMap {
      case right @ Right(_) =>
        // Single atomic modify: capture old state and compute new state together,
        // eliminating the TOCTOU between get + updateAndGet.
        stateRef
          .modify { s =>
            val next = s match {
              case HalfOpen    => Closed(0)
              case Closed(_)   => Closed(0)
              case o @ Open(_) => o
            }
            val wasUnhealthy = s match {
              case Closed(0) => false
              case _         => true
            }
            (next, wasUnhealthy)
          }
          .flatMap { wasUnhealthy =>
            if (wasUnhealthy) Sync[F].delay(log.info("Circuit breaker closed — upstream recovered"))
            else Concurrent[F].unit
          }
          .as(right)
      case left @ Left(_) =>
        stateRef
          .modify { s =>
            val next = s match {
              case HalfOpen                          => Open(now)
              case Closed(n) if n + 1 >= maxFailures => Open(now)
              case Closed(n)                         => Closed(n + 1)
              case o @ Open(_)                       => o
            }
            (next, (s, next))
          }
          .flatMap {
            case (HalfOpen, Open(_)) =>
              Sync[F].delay(log.warn("Circuit breaker re-opened — probe request failed"))
            case (_, Open(_)) =>
              Sync[F].delay(log.warn("Circuit breaker opened — upstream failure threshold reached"))
            case (_, Closed(n)) =>
              Sync[F].delay(log.warn(s"Upstream failure recorded (consecutive failures: $n)"))
            case _ =>
              Concurrent[F].unit
          }
          .as(left)
    }
}

private[interpreters] object CircuitBreaker {
  def apply[F[_]: Concurrent: Clock](maxFailures: Int, resetTimeoutMs: Long): F[CircuitBreaker[F]] =
    Ref.of[F, CBState](CBState.Closed(0)).map(new CircuitBreaker[F](maxFailures, resetTimeoutMs, _))
}
