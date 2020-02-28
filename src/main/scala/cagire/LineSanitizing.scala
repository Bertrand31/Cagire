package cagire

object LineSanitizing {

  def lineToWords: String => Array[String] =
    _
      .split("\\P{L}+") // Split on everything that isn't a unicode letter
      .filterNot(_.isEmpty)
      .map(_.toLowerCase)
}
