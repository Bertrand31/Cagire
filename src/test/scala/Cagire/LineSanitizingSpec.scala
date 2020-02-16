import cagire.LineSanitizing
import org.scalatest.flatspec.AnyFlatSpec

class LineSanitizingSpec extends AnyFlatSpec {

  behavior of "The LineSanitizing helper methods"

  behavior of "the lineToWords method"

  val line = "This. Is|a|sample/and quite complicated 123.13 linè"

  it should "return an array of sanitized words" in {

    val words = LineSanitizing.lineToWords(line).toList
    assert(words == List("this", "is", "a", "sample", "and", "quite", "complicated", "linè"))
  }
}
