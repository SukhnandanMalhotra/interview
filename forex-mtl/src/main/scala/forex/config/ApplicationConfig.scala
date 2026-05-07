package forex.config

import scala.concurrent.duration.FiniteDuration

case class ApplicationConfig(
    http: HttpConfig,
    oneFrame: OneFrameConfig
)

case class HttpConfig(
    host: String,
    port: Int,
    timeout: FiniteDuration
)

case class OneFrameConfig(
    host: String,
    port: Int,
    token: String,
    cacheTtlMinutes: Int,
    cbMaxFailures: Int,
    cbResetTimeoutSeconds: Long
) {
  require(cacheTtlMinutes >= 1 && cacheTtlMinutes <= 5,
    s"cacheTtlMinutes must be between 1 and 5 (got $cacheTtlMinutes) — rates must not be older than 5 minutes")

  def cacheTtlMillis: Long       = cacheTtlMinutes * 60 * 1000L
  def cbResetTimeoutMillis: Long = cbResetTimeoutSeconds * 1000L
}
