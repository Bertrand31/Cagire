package cagire

import scala.util.Try
import scala.concurrent.Future
import scala.collection.immutable.ArraySeq
import io.circe.syntax.EncoderOps
import io.circe.parser.decode
import utils.FileUtils

final case class InvertedIndex(index: Map[String, Map[Int, ArraySeq[Int]]] = Map()) {

  def addLine(docId: Int, lineNumber: Int, words: Array[String]): InvertedIndex = {
    val newIndex = words.foldLeft(this.index)((acc, word) => {
      val wordOccurences = acc.getOrElse(word, Map())
      val currentMatches = wordOccurences.getOrElse(docId, ArraySeq()) :+ lineNumber
      val documentAndLinePair = wordOccurences + (docId -> currentMatches)
      acc + (word -> documentAndLinePair)
    })
    this.copy(index=newIndex)
  }

  def keys = this.index.keys.toIndexedSeq

  def searchWord(word: String): Map[Int, ArraySeq[Int]] =
    index.getOrElse(word.toLowerCase, Map())

  def commitToDisk(): Future[Unit] =
    FileUtils.writeFileAsync("inverted_index.json", this.index.asJson.noSpaces)
}

object InvertedIndex {

  private def InvertedIndexFileName = "inverted_index.json"

  def hydrate(): Try[InvertedIndex] = {
    val invertedIndexFile = FileUtils.readFile(InvertedIndexFileName)
    decode[Map[String, Map[Int, ArraySeq[Int]]]](invertedIndexFile)
      .toTry
      .map(InvertedIndex(_))
  }
}
