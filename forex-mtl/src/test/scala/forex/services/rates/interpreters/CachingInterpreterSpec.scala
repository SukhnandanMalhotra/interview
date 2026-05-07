package forex.services.rates.interpreters

import cats.effect.{ ContextShift, IO, Timer }
import cats.effect.concurrent.{ Deferred, Ref }
import forex.domain.{ Currency, Price, Rate, Timestamp }
import forex.services.rates.errors.Error
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class CachingInterpreterSpec extends AnyFunSuite with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO]     = IO.timer(ExecutionContext.global)

  private val usdJpy = Rate.Pair(Currency.USD, Currency.JPY)
  private val eurGbp = Rate.Pair(Currency.EUR, Currency.GBP)

  private val sampleRates: Map[Rate.Pair, Rate] = Map(
    usdJpy -> Rate(usdJpy, Price(BigDecimal("0.5")), Timestamp.now),
    eurGbp -> Rate(eurGbp, Price(BigDecimal("0.7")), Timestamp.now)
  )

  // Thread-safe fake fetcher — uses Ref[IO, Int] instead of a plain var so call-count
  // reads are safe under concurrent fibers.
  class FakeFetcher(rates: Map[Rate.Pair, Rate], countRef: Ref[IO, Int]) extends RateFetcher[IO] {
    def callCount: IO[Int] = countRef.get
    override def fetchAll(): IO[Either[Error, Map[Rate.Pair, Rate]]] =
      countRef.update(_ + 1).as(Right(rates))
  }

  object FakeFetcher {
    def apply(rates: Map[Rate.Pair, Rate]): IO[FakeFetcher] =
      Ref.of[IO, Int](0).map(new FakeFetcher(rates, _))
  }

  // Not private so Scala 2's -Ywarn-unused:privates does not fire on the default argument.
  def makeInterpreter(fetcher: RateFetcher[IO], ttl: Long = CachingInterpreter.ttlMillis) =
    (for {
      cacheRef    <- Ref.of[IO, Option[CacheEntry]](None)
      inFlightRef <- Ref.of[IO, Option[Deferred[IO, Either[Error, CacheEntry]]]](None)
      cb          <- CircuitBreaker[IO](maxFailures = 5, resetTimeoutMs = 30_000L)
    } yield new CachingInterpreter[IO](fetcher, ttl, cacheRef, inFlightRef, cb)).unsafeRunSync()

  test("returns rate from fetcher on first call") {
    val fetcher     = FakeFetcher(sampleRates).unsafeRunSync()
    val interpreter = makeInterpreter(fetcher)

    val result = interpreter.get(usdJpy).unsafeRunSync()

    result shouldBe Right(sampleRates(usdJpy))
    fetcher.callCount.unsafeRunSync() shouldBe 1
  }

  test("serves subsequent requests from cache without calling fetcher again") {
    val fetcher     = FakeFetcher(sampleRates).unsafeRunSync()
    val interpreter = makeInterpreter(fetcher)

    interpreter.get(usdJpy).unsafeRunSync()
    interpreter.get(usdJpy).unsafeRunSync()
    val result = interpreter.get(eurGbp).unsafeRunSync()

    result shouldBe Right(sampleRates(eurGbp))
    fetcher.callCount.unsafeRunSync() shouldBe 1
  }

  test("refreshes cache after TTL expires") {
    val fetcher     = FakeFetcher(sampleRates).unsafeRunSync()
    val interpreter = makeInterpreter(fetcher, ttl = 100L)

    interpreter.get(usdJpy).unsafeRunSync()
    IO.sleep(200.millis).unsafeRunSync()
    interpreter.get(usdJpy).unsafeRunSync()

    fetcher.callCount.unsafeRunSync() shouldBe 2
  }

  test("returns Left for pair not in fetcher response") {
    val result = makeInterpreter(FakeFetcher(Map.empty).unsafeRunSync()).get(usdJpy).unsafeRunSync()
    result.isLeft shouldBe true
  }

  test("retries on transient failure and returns result on eventual success") {
    // Fetcher fails the first 2 calls; succeeds on the 3rd (within retryN(3)).
    val countRef = Ref.of[IO, Int](0).unsafeRunSync()
    val interpreter = makeInterpreter(new RateFetcher[IO] {
      override def fetchAll(): IO[Either[Error, Map[Rate.Pair, Rate]]] =
        countRef.updateAndGet(_ + 1).map { n =>
          if (n < 3) Left(Error.RateServiceUnavailable)
          else Right(sampleRates)
        }
    })
    val result = interpreter.get(usdJpy).unsafeRunSync()
    result shouldBe Right(sampleRates(usdJpy))
  }

  test("returns Left after all retries exhausted") {
    val result = makeInterpreter(new RateFetcher[IO] {
      override def fetchAll(): IO[Either[Error, Map[Rate.Pair, Rate]]] =
        IO.pure(Left(Error.RateServiceUnavailable))
    }).get(usdJpy).unsafeRunSync()
    result.isLeft shouldBe true
  }

  test("concurrent requests on empty cache trigger exactly one upstream fetch (thundering herd)") {
    import cats.implicits._

    val countRef = Ref.of[IO, Int](0).unsafeRunSync()
    // 50ms sleep gives all N fibers time to start and observe the in-flight Deferred
    // before the first fetch completes, proving no thundering herd.
    val slowFetcher = new RateFetcher[IO] {
      override def fetchAll(): IO[Either[Error, Map[Rate.Pair, Rate]]] =
        countRef.update(_ + 1) *> IO.sleep(50.millis).as(Right(sampleRates))
    }
    val interpreter = makeInterpreter(slowFetcher)

    val N = 50
    val results = (1 to N).toList
      .traverse(_ => interpreter.get(usdJpy).start)
      .flatMap(_.traverse(_.join))
      .unsafeRunSync()

    results.forall(_ == Right(sampleRates(usdJpy))) shouldBe true
    countRef.get.unsafeRunSync() shouldBe 1
  }
}
