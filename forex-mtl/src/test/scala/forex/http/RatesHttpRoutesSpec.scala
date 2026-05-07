package forex.http

import forex.domain.Currency
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RatesHttpRoutesSpec extends AnyFunSuite with Matchers {

  test("Currency.fromString returns Right for valid currency") {
    Currency.fromString("USD") shouldBe Right(Currency.USD)
    Currency.fromString("JPY") shouldBe Right(Currency.JPY)
    Currency.fromString("EUR") shouldBe Right(Currency.EUR)
  }

  test("Currency.fromString returns Left for invalid currency") {
    Currency.fromString("INVALID").isLeft shouldBe true
    Currency.fromString("").isLeft shouldBe true
    Currency.fromString("US").isLeft shouldBe true
  }

  test("Currency.fromString is case insensitive") {
    Currency.fromString("usd") shouldBe Right(Currency.USD)
    Currency.fromString("USD") shouldBe Right(Currency.USD)
    Currency.fromString("Usd") shouldBe Right(Currency.USD)
  }

  test("all supported currencies parse correctly") {
    val supported = List("AUD", "CAD", "CHF", "EUR", "GBP", "NZD", "JPY", "SGD", "USD")
    supported.foreach { code =>
      Currency.fromString(code).isRight shouldBe true
    }
  }
}
