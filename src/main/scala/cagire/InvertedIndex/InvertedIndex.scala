package cagire

import java.io.FileWriter
import scala.util.Try
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

  private val ChunkSize = 10000

  def commitToDisk(): Unit = {
    val fw = new FileWriter(InvertedIndexFilePath)
    this.index
      .sliding(ChunkSize, ChunkSize)
      .foreach(chunk => {
        fw.write {
          chunk
            .map(line => s"${line._1};${line._2.asJson.noSpaces}")
            .mkString("\n") + "\n"
        }
    })
    fw.close
  }
}

object InvertedIndex {

  private def InvertedIndexFilePath = StoragePath + "/inverted_index.csv"

  def hydrate(): Try[InvertedIndex] =
    FileUtils
      .readFile(InvertedIndexFilePath)
      .map(
        _.map(line => {
          val Array(key, value) = line.split(';')
          (key -> decode[Map[Int, Set[Int]]](value).right.get)
        }).toMap
      )
      .map(InvertedIndex(_))
}
