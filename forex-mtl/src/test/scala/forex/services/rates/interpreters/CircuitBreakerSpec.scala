package forex.services.rates.interpreters

import cats.effect.{ ContextShift, IO, Timer }
import forex.services.rates.errors.Error
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class CircuitBreakerSpec extends AnyFunSuite with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]     = IO.timer(ExecutionContext.global)

  private val upstreamFail: IO[Either[Error, Unit]] =
    IO.pure(Left(Error.RateServiceUnavailable))

  private val upstreamSucceed: IO[Either[Error, Unit]] =
    IO.pure(Right(()))

  private def makeCB(maxFailures: Int = 2, resetTimeoutMs: Long = 30_000L): CircuitBreaker[IO] =
    CircuitBreaker[IO](maxFailures, resetTimeoutMs).unsafeRunSync()

  test("allows requests through when circuit is closed") {
    val result = makeCB().protect(upstreamSucceed).unsafeRunSync()
    result shouldBe Right(())
  }

  test("opens circuit after maxFailures consecutive failures") {
    val cb = makeCB(maxFailures = 2)
    cb.protect(upstreamFail).unsafeRunSync()  // Closed(1)
    cb.protect(upstreamFail).unsafeRunSync()  // Open

    val result = cb.protect(upstreamSucceed).unsafeRunSync()
    result shouldBe Left(Error.RateServiceUnavailable)
  }

  test("resets failure count to zero on success") {
    val cb = makeCB(maxFailures = 2)
    cb.protect(upstreamFail).unsafeRunSync()    // Closed(1)
    cb.protect(upstreamSucceed).unsafeRunSync() // Closed(0)
    cb.protect(upstreamFail).unsafeRunSync()    // Closed(1) — did NOT open
    val result = cb.protect(upstreamSucceed).unsafeRunSync()
    result shouldBe Right(())
  }

  test("allows one probe request after reset timeout and closes circuit on success") {
    val cb = makeCB(maxFailures = 1, resetTimeoutMs = 100L)
    cb.protect(upstreamFail).unsafeRunSync()  // → Open

    IO.sleep(150.millis).unsafeRunSync()

    // Probe fires and succeeds → back to Closed
    val probeResult = cb.protect(upstreamSucceed).unsafeRunSync()
    probeResult shouldBe Right(())

    // Circuit is now closed — normal requests allowed
    cb.protect(upstreamSucceed).unsafeRunSync() shouldBe Right(())
  }

  test("returns to Open with a fresh timer when probe fails") {
    val cb = makeCB(maxFailures = 1, resetTimeoutMs = 100L)
    cb.protect(upstreamFail).unsafeRunSync()  // → Open

    IO.sleep(150.millis).unsafeRunSync()
    cb.protect(upstreamFail).unsafeRunSync()  // probe fires, fails → Open(now) with fresh timer

    // Should be Open again — probe must NOT fire immediately
    val result = cb.protect(upstreamSucceed).unsafeRunSync()
    result shouldBe Left(Error.RateServiceUnavailable)
  }

  test("rejects concurrent requests while probe is in-flight (HalfOpen)") {
    // Probabilistic: relies on r2 being scheduled before the 50ms probe sleep expires.
    // This holds reliably on any non-pathologically loaded machine. A fully deterministic
    // version would require cats-effect-laws TestContext with a controlled clock.
    val cb = makeCB(maxFailures = 1, resetTimeoutMs = 100L)
    cb.protect(upstreamFail).unsafeRunSync()  // → Open

    IO.sleep(150.millis).unsafeRunSync()

    // Probe sleeps 50ms, giving the concurrent request time to see HalfOpen and be rejected
    val result = for {
      fiber <- cb.protect(IO.sleep(50.millis) *> upstreamSucceed).start
      r2    <- cb.protect(upstreamSucceed)
      r1    <- fiber.join
    } yield (r1, r2)

    val (r1, r2) = result.unsafeRunSync()

    // One of them is the probe (Right), the other was rejected (Left)
    val results = Set(r1, r2)
    results should contain(Right(()))
    results should contain(Left(Error.RateServiceUnavailable))
  }
}
