package cagire

import scala.annotation.tailrec
import scala.util.Try
import scala.util.hashing.MurmurHash3
import scala.collection.mutable.PriorityQueue
import cats.implicits._
import utils.FileUtils

object DocumentHandling {

  /** We generate a filename from hashing the first 10 lines of the file.
    * The number of 10 is arbitrary, but it allows us to be reasonably safe from collisions
    * without having to load the whole file and waste too much time on filename generation.
    */
  private def genFilename: String => Try[Int] =
    FileUtils.readFile(_)
      .map(_ take 10)
      .map(MurmurHash3.orderedHash)

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
    targets: PriorityQueue[Int],
    current: Int = 1,
    soFar: Map[Int, String] = Map(),
  )(document: Iterator[String]): Map[Int, String] =
    if (targets.isEmpty) soFar
    else {
      val currentTarget = targets.dequeue
      document.drop(currentTarget - current)
      val targetLine = (currentTarget, document.next)
      loadLines(targets, currentTarget + 1, soFar + targetLine)(document)
    }

    def loadLinesFromDocument(documentId: Int, targets: Seq[Int]): Try[Map[Int, String]] = {
      val targetsMinHeap = PriorityQueue(targets:_*)(Ordering[Int].reverse)
      loadDocument(documentId) map loadLines(targetsMinHeap)
    }
}
