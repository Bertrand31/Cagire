package cagire

import scala.annotation.tailrec
import scala.util.Try
import scala.collection.mutable.PriorityQueue
import cats.implicits._
import utils.FileUtils

object DocumentHandling {

  private def loadDocument(documentId: Int): Try[Iterator[String]] =
    FileUtils.readFile(StoragePath |+| documentId.toString)

  def loadDocumentWithLinesNumbers(documentId: Int): Try[Iterator[(Int, String)]] =
    loadDocument(documentId) map (Iterator.from(1) zip _.to(Iterator))

  /** Loads the required lines from a lazy iterator without holding more than one line
    * in memory at any given point (except from the ones being accumulated).
    * This method is made public for testing purposes.
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
            FileUtils.readFile(s"$StoragePath${documentId.toString}/$documentNumber")
              .map(loadLines(targetsMinHeap))
              .map(_.map({ case (lineNb, line) => (toAbsoluteLine(documentNumber, lineNb), line) }))
          }
        })
        .to(LazyList)
        .sequence
        .map(_ foldMap identity)
}
