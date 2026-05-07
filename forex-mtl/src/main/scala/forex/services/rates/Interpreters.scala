package forex.services.rates

import cats.Applicative
import cats.effect.{ Clock, Concurrent, Sync, Timer }
import forex.config.OneFrameConfig
import interpreters._
import org.http4s.client.Client

object Interpreters {
  def dummy[F[_]: Applicative]: Algebra[F] = new OneFrameDummy[F]()

  def live[F[_]: Sync: Clock](config: OneFrameConfig, httpClient: Client[F]): Algebra[F] =
    new OneFrameClient[F](config, httpClient)

  def cached[F[_]: Concurrent: Clock: Timer](config: OneFrameConfig, httpClient: Client[F]): F[Algebra[F]] =
    CachingInterpreter[F](new OneFrameClient[F](config, httpClient), config)
}
