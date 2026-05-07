package forex.services.rates.interpreters

import forex.domain.Rate
import forex.services.rates.errors.Error

// Abstraction for fetching all pairs at once — decouples CachingInterpreter from OneFrameClient
trait RateFetcher[F[_]] {
  def fetchAll(): F[Error Either Map[Rate.Pair, Rate]]
}
