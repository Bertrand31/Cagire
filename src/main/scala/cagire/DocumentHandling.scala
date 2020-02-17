package cagire

import scala.util.hashing.MurmurHash3
import scala.annotation.tailrec
import scala.util.Try
import scala.collection.mutable.PriorityQueue
import cats.implicits._
import utils.FileUtils

object DocumentHandling {

  def getSplitDocument(path: String): Try[(Int, Iterator[(Seq[String], Int)])] =
    FileUtils.readFile(path)
      .map(_.sliding(ChunkSize, ChunkSize).zip(Iterator from 0))
      .map(fileIterator => {
        val head = fileIterator.next
        val documentId = MurmurHash3.orderedHash(head._1)
        val completeIterator = (Iterator(head) ++ fileIterator)
        (documentId, completeIterator)
      })

  /** Loads the required lines from a lazy iterator without holding more than one line
    * in memory at any given point (except from the ones being accumulated).
    * This method is public only for testing purposes.
    */
  @tailrec
  def loadLines(
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

    private def toChunkNumber: Int => Int = _ / ChunkSize

    private def toRelativeLine: Int => Int = _ % ChunkSize

    private def toAbsoluteLine(chunkNumber: Int, lineNumber: Int): Int =
      chunkNumber * ChunkSize + lineNumber

    def loadLinesFromDocument(documentId: Int, targets: Seq[Int]): Try[Map[Int, String]] =
      targets
        .groupBy(toChunkNumber)
        .map({
          case (documentNumber, lineNumbers) => {
            val absoluteLineNumbers = lineNumbers map toRelativeLine
            val targetsMinHeap = PriorityQueue(absoluteLineNumbers:_*)(Ordering[Int].reverse)
            FileUtils.readFile(s"$StoragePath$documentId/$documentNumber")
              .map(loadLines(targetsMinHeap))
              .map(_.map({ case (lineNb, line) => (toAbsoluteLine(documentNumber, lineNb), line) }))
          }
        })
        .to(LazyList)
        .sequence
        .map(_ foldMap identity)
}
