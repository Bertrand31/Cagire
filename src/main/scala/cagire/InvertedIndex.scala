package cagire

import scala.collection.immutable.ArraySeq

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

  def searchWord(word: String): Map[Int, ArraySeq[Int]] =
    index.getOrElse(word.toLowerCase, Map())
}
