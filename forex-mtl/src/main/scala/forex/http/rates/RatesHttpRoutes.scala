package forex.http
package rates

import cats.effect.Sync
import cats.implicits._
import forex.programs.RatesProgram
import forex.programs.rates.{ Protocol => RatesProgramProtocol }
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router

class RatesHttpRoutes[F[_]: Sync](rates: RatesProgram[F]) extends Http4sDsl[F] {

  import Converters._, QueryParams._, Protocol._

  private[http] val prefixPath = "/rates"

  private val httpRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root :? FromQueryParam(fromOpt) +& ToQueryParam(toOpt) =>
      (fromOpt, toOpt) match {
        case (None, _) => BadRequest("Query parameter 'from' is required")
        case (_, None) => BadRequest("Query parameter 'to' is required")
        case (Some(fromV), Some(toV)) =>
          (fromV, toV)
            .mapN(RatesProgramProtocol.GetRatesRequest)
            .fold(
              errors => BadRequest(errors.map(_.sanitized).mkString_(", ")),
              request =>
                if (request.from == request.to)
                  BadRequest("Cannot convert a currency to itself")
                else
                  rates.get(request).flatMap {
                    case Right(rate) => Ok(rate.asGetApiResponse)
                    case Left(_) =>
                      ServiceUnavailable("Exchange rate service is temporarily unavailable. Please try again shortly.")
                }
            )
      }
  }

  val routes: HttpRoutes[F] = Router(
    prefixPath -> httpRoutes
  )

}
