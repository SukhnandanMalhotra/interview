package forex.http.rates

import cats.effect.{ ContextShift, IO }
import forex.domain.{ Currency, Price, Rate, Timestamp }
import forex.programs.RatesProgram
import forex.programs.rates.{ Protocol => RatesProgramProtocol, errors }
import org.http4s._
import org.http4s.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class RatesHttpRoutesIntegrationSpec extends AnyFunSuite with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val usdJpy     = Rate.Pair(Currency.USD, Currency.JPY)
  private val sampleRate = Rate(usdJpy, Price(BigDecimal("0.8963")), Timestamp.now)

  private def makeProgram(result: Either[errors.Error, Rate]): RatesProgram[IO] =
    new RatesProgram[IO] {
      def get(req: RatesProgramProtocol.GetRatesRequest): IO[Either[errors.Error, Rate]] =
        IO.pure(result)
    }

  private def makeRoutes(program: RatesProgram[IO]): HttpRoutes[IO] =
    new RatesHttpRoutes[IO](program).routes

  private def runRequest(routes: HttpRoutes[IO], request: Request[IO]): Response[IO] =
    routes.run(request).value.unsafeRunSync().getOrElse(Response.notFound)

  private val successProgram  = makeProgram(Right(sampleRate))
  private val unavailableProgram = makeProgram(Left(errors.Error.RateServiceUnavailable))

  test("returns 200 for a valid currency pair") {
    val response = runRequest(makeRoutes(successProgram), Request[IO](Method.GET, uri"/rates?from=USD&to=JPY"))
    response.status shouldBe Status.Ok
  }

  test("returns 400 when from and to are the same currency") {
    val response = runRequest(makeRoutes(successProgram), Request[IO](Method.GET, uri"/rates?from=USD&to=USD"))
    response.status shouldBe Status.BadRequest
    response.as[String].unsafeRunSync() shouldBe "Cannot convert a currency to itself"
  }

  test("returns 400 for an invalid from currency code") {
    val response = runRequest(makeRoutes(successProgram), Request[IO](Method.GET, uri"/rates?from=INVALID&to=JPY"))
    response.status shouldBe Status.BadRequest
    response.as[String].unsafeRunSync() should include("INVALID")
  }

  test("returns 400 for an invalid to currency code") {
    val response = runRequest(makeRoutes(successProgram), Request[IO](Method.GET, uri"/rates?from=USD&to=BOGUS"))
    response.status shouldBe Status.BadRequest
  }

  test("returns 503 when the rate service is unavailable") {
    val response = runRequest(makeRoutes(unavailableProgram), Request[IO](Method.GET, uri"/rates?from=USD&to=JPY"))
    response.status shouldBe Status.ServiceUnavailable
    response.as[String].unsafeRunSync() shouldBe "Exchange rate service is temporarily unavailable. Please try again shortly."
  }

  test("returns 400 with message when from parameter is missing") {
    val response = runRequest(makeRoutes(successProgram), Request[IO](Method.GET, uri"/rates?to=JPY"))
    response.status shouldBe Status.BadRequest
    response.as[String].unsafeRunSync() shouldBe "Query parameter 'from' is required"
  }

  test("returns 400 with message when to parameter is missing") {
    val response = runRequest(makeRoutes(successProgram), Request[IO](Method.GET, uri"/rates?from=USD"))
    response.status shouldBe Status.BadRequest
    response.as[String].unsafeRunSync() shouldBe "Query parameter 'to' is required"
  }
}
