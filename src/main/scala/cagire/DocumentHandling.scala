package cagire

import scala.annotation.tailrec
import scala.util.Try
import scala.util.hashing.MurmurHash3
import cats.implicits._
import utils.FileUtils

object DocumentHandling {

  private def genFilename: String => Try[Int] = FileUtils.readFile(_) map MurmurHash3.orderedHash

  def storeDocument(path: String): Try[Int] = {
    val filename = genFilename(path)
    filename foreach ((StoragePath + _) >>> (FileUtils.copy(path, _)))
    filename
  }

  private def loadDocument(documentId: Int): Try[Iterator[String]] =
    FileUtils.readFile(StoragePath |+| documentId.toString)

  def loadDocumentWithLinesNumbers(documentId: Int): Try[Stream[(Int, String)]] =
    loadDocument(documentId) map (Stream.from(1) zip _.toStream)

  /** Loads the required lines from a lazy iterator without holding more than one line
    * in memory at any given point (except from the ones being accumulated).
    */
  @tailrec
  private def loadLines(
    document: Iterator[String], targets: Set[Int], current: Int, soFar: Map[Int, String],
  ): Map[Int, String] =
    if (targets.isEmpty) soFar
    else {
      val head = document.next
      if (targets contains current) {
        val newSoFar = soFar + (current -> head)
        if (!document.hasNext) newSoFar
        else loadLines(document, targets - current, current + 1, newSoFar)
      } else {
        if (!document.hasNext) soFar
        else loadLines(document, targets, current + 1, soFar)
      }
    }

    def loadLinesFromDocument(documentId: Int, targets: Set[Int]): Try[Map[Int, String]] =
      loadDocument(documentId) map (loadLines(_, targets, 1, Map()))
}
