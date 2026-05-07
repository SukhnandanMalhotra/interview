package forex.services.rates.interpreters

import cats.effect.{ Clock, Sync }
import cats.implicits._
import org.slf4j.LoggerFactory
import forex.config.OneFrameConfig
import forex.domain.{ Currency, Price, Rate, Timestamp }
import forex.services.rates.Algebra
import forex.services.rates.errors.Error
import io.circe.generic.auto._
import org.http4s.Headers
import org.http4s.Uri
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client

import java.time.{ Instant, OffsetDateTime, ZoneOffset }
import java.util.concurrent.TimeUnit

class OneFrameClient[F[_]: Sync: Clock](config: OneFrameConfig, httpClient: Client[F])
    extends Algebra[F]
    with RateFetcher[F] {

  private val log = LoggerFactory.getLogger(getClass)

  private val allPairs: List[Rate.Pair] =
    for {
      from <- Currency.values
      to <- Currency.values
      if from != to
    } yield Rate.Pair(from, to)

  private case class OneFrameRate(
      from: String,
      to: String,
      price: BigDecimal,
      time_stamp: String
  )

  // Fetches all 72 pairs in one HTTP call — used by CachingInterpreter on cache refresh
  def fetchAll(): F[Error Either Map[Rate.Pair, Rate]] = {
    val pairsQuery = allPairs
      .map(p => s"pair=${Currency.show.show(p.from)}${Currency.show.show(p.to)}")
      .mkString("&")

    val uri = Uri.unsafeFromString(
      s"http://${config.host}:${config.port}/rates?$pairsQuery"
    )

    val request = org.http4s.Request[F](
      uri = uri,
      headers = Headers(org.http4s.Header.Raw(org.typelevel.ci.CIString("token"), config.token))
    )

    httpClient
      .expect[List[OneFrameRate]](request)
      .flatMap { rates =>
        Clock[F].realTime(TimeUnit.MILLISECONDS).map { nowMs =>
          val ts = Timestamp(OffsetDateTime.ofInstant(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC))
          val ratesMap = rates.flatMap { r =>
            for {
              from <- Currency.values.find(c => Currency.show.show(c) == r.from)
              to <- Currency.values.find(c => Currency.show.show(c) == r.to)
            } yield {
              val pair = Rate.Pair(from, to)
              pair -> Rate(pair, Price(r.price), ts)
            }
          }.toMap
          Right(ratesMap): Either[Error, Map[Rate.Pair, Rate]]
        }
      }
      .handleErrorWith { e =>
        // Log the full internal error for debugging; never expose host/port details to callers.
        Sync[F]
          .delay(log.error(s"One-Frame HTTP call failed: ${Option(e.getMessage).getOrElse(e.toString)}"))
          .as(Left(Error.RateServiceUnavailable))
      }
  }

  override def get(pair: Rate.Pair): F[Error Either Rate] =
    fetchAll().map {
      case Right(ratesMap) =>
        ratesMap.get(pair).toRight(Error.RateServiceUnavailable)
      case Left(err) =>
        Left(err)
    }
}
