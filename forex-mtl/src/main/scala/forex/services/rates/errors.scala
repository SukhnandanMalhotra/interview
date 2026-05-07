package forex.services.rates

object errors {

  sealed trait Error
  object Error {
    // Infrastructure failures — circuit breaker open, connection refused, timeouts.
    // Internal details are logged but never surfaced to callers.
    case object RateServiceUnavailable extends Error

    final case class OneFrameLookupFailed(msg: String) extends Error
  }

}
