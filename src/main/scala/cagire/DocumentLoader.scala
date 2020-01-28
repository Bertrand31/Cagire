package data_structures

import scala.io.Source

object DocumentLoader {

  private val BaseDirectory = "src/main/resources"

  def loadDocument(documentId: Int): Iterator[String] =
    Source
      .fromFile(s"$BaseDirectory/${documentId.toString}")
      .getLines

  def loadDocumentWithLinesNumbers(documentId: Int): IndexedSeq[(Int, String)] = {
    val lines = loadDocument(documentId).toArray
    (1 to lines.length) zip lines
  }
}
