package data_structures

object LineSanitizing {

  def lineToWords: String => Array[String] =
    _
      .split("\\|| |/|\\[|\\]|\\{|\\}|\\\\|\\(|\\)|=| ")
      .map(_.filter(_.isLetter))
      .filterNot(_.isEmpty)
      .map(_.toLowerCase)
}
