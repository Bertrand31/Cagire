package cagire

import scala.collection.immutable.ArraySeq
import io.circe.parser.decode
import utils.FileUtils

object Main extends App {

  val documentsIndexFile = FileUtils.readFile("documents_index.json")
  val invertedIndexFile = FileUtils.readFile("inverted_index.json")

  for {
    documentsIndexMap <- decode[Map[Int, String]](documentsIndexFile)
    invertedIndexMap <- decode[Map[String, Map[Int, ArraySeq[Int]]]](invertedIndexFile)
  } {
    val documentsIndex = DocumentsIndex(documentsIndexMap)
    val invertedIndex = InvertedIndex(invertedIndexMap)
    val indexesTrie = IndexesTrie() ++ invertedIndex.keys
    val cagire = Cagire(documentsIndex, invertedIndex, indexesTrie)
    cagire.searchPrefixAndShow("sim")
  }
}
