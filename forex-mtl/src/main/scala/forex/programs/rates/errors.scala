package forex.programs.rates

import forex.services.rates.errors.{ Error => RatesServiceError }

object errors {

  sealed trait Error extends Exception
  object Error {
    case object RateServiceUnavailable extends Error {
      override def getMessage: String = "Exchange rate service is temporarily unavailable"
    }
  }

  def toProgramError(error: RatesServiceError): Error = error match {
    case RatesServiceError.RateServiceUnavailable    => Error.RateServiceUnavailable
    case RatesServiceError.OneFrameLookupFailed(_)   => Error.RateServiceUnavailable
  }
}
