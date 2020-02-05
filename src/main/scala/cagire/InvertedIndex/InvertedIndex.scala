package cagire

import scala.util.Try
import cats.implicits._
import io.circe.syntax.EncoderOps
import io.circe.parser.decode
import utils.FileUtils

final case class InvertedIndex(index: Map[String, Map[Int, Set[Int]]] = Map()) {

  import InvertedIndex.InvertedIndexFilePath

  def addLine(docId: Int, lineNumber: Int, words: Array[String]): InvertedIndex = {
    val newIndex = words.foldLeft(this.index)((acc, word) => {
      val wordOccurences = acc.getOrElse(word, Map())
      val currentMatches = wordOccurences.getOrElse(docId, Set()) + lineNumber
      val documentAndLinePair = wordOccurences + (docId -> currentMatches)
      acc + (word -> documentAndLinePair)
    })
    this.copy(index=newIndex)
  }

  def keys = this.index.keys.toIndexedSeq

  def searchWord(word: String): Map[Int, Set[Int]] =
    index.getOrElse(word.toLowerCase, Map())

  def commitToDisk(): Unit =
    FileUtils.writeCSVProgressively(
      InvertedIndexFilePath,
      this.index
        .view
        .map({ case (word, matches) => s"$word;${matches.asJson.noSpaces}" })
    )
}

object InvertedIndex {

  private def InvertedIndexFilePath = StoragePath |+| "inverted_index.csv"

  private def decodeIndexLine: Iterator[String] => Try[Map[String, Map[Int, Set[Int]]]] =
    _
      .toList
      .map(line => {
        val Array(word, matchesStr) = line.split(';')
        decode[Map[Int, Set[Int]]](matchesStr).map((word -> _))
      })
      .sequence
      .map(_.toMap)
      .toTry

  def hydrate(): Try[InvertedIndex] =
    FileUtils
      .readFile(InvertedIndexFilePath)
      .flatMap(decodeIndexLine)
      .map(InvertedIndex(_))
}
