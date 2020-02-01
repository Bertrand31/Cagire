package cagire

import scala.util.Try
import scala.concurrent.Future
import io.circe.syntax.EncoderOps
import io.circe.parser.decode
import utils.FileUtils

final case class InvertedIndex(index: Map[String, Map[Int, Set[Int]]] = Map()) {

   import InvertedIndex._

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

  def commitToDisk(): Future[Unit] =
    FileUtils.writeFileAsync(InvertedIndexFilePath, this.index.asJson.noSpaces)
}

object InvertedIndex {

  private def InvertedIndexFilePath = StoragePath + "/inverted_index.json"

  private def decodeFile: String => Try[Map[String, Map[Int, Set[Int]]]] =
    decode[Map[String, Map[Int, Set[Int]]]](_).toTry

  def hydrate(): Try[InvertedIndex] = {
    FileUtils
      .readFile(InvertedIndexFilePath)
      .map(_.mkString)
      .flatMap(decodeFile)
      .map(InvertedIndex(_))
  }
}
