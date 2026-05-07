package forex

import cats.effect.{ Clock, ConcurrentEffect, Resource, Timer }
import forex.config.ApplicationConfig
import forex.http.rates.RatesHttpRoutes
import forex.services._
import forex.programs._
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.implicits._
import org.http4s.server.middleware.{ AutoSlash, Timeout }
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class Module[F[_]: ConcurrentEffect: Timer](config: ApplicationConfig, ratesService: RatesService[F]) {

  private val ratesProgram: RatesProgram[F] = RatesProgram[F](ratesService)

  private val ratesHttpRoutes: HttpRoutes[F] = new RatesHttpRoutes[F](ratesProgram).routes

  type PartialMiddleware = HttpRoutes[F] => HttpRoutes[F]
  type TotalMiddleware   = HttpApp[F] => HttpApp[F]

  private val routesMiddleware: PartialMiddleware = {
    { http: HttpRoutes[F] =>
      AutoSlash(http)
    }
  }

  private val appMiddleware: TotalMiddleware = { http: HttpApp[F] =>
    Timeout(config.http.timeout)(http)
  }

  private val http: HttpRoutes[F] = ratesHttpRoutes

  val httpApp: HttpApp[F] = appMiddleware(routesMiddleware(http).orNotFound)

}

object Module {
  def resource[F[_]: ConcurrentEffect: Timer: Clock](config: ApplicationConfig): Resource[F, Module[F]] =
    BlazeClientBuilder[F](ExecutionContext.global).withRequestTimeout(10.seconds).resource.flatMap { client =>
      Resource.eval(RatesServices.cached[F](config.oneFrame, client)).map { ratesService =>
        new Module[F](config, ratesService)
      }
    }
}
