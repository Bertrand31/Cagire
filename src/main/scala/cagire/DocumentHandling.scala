package cagire

import java.io.{File, PrintWriter}
import scala.util.hashing.MurmurHash3
import scala.annotation.tailrec
import scala.util.{Try, Using}
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
          case (chunkNumber, lineNumbers) => {
            val absoluteLineNumbers = lineNumbers map toRelativeLine
            val targetsMinHeap = PriorityQueue(absoluteLineNumbers:_*)(Ordering[Int].reverse)
            FileUtils.readFile(s"$StoragePath$documentId/$chunkNumber")
              .map(loadLines(targetsMinHeap))
              .map(_.map({ case (lineNb, line) => (toAbsoluteLine(chunkNumber, lineNb), line) }))
          }
        })
        .to(LazyList)
        .sequence
        .map(_ foldMap identity)

  def writeChunks[A](
    documentId: Int,
    chunks: Iterator[(Seq[String], Int)],
    accumulator: A,
    accFn: (A, (String, Int)) => A,
  ): A = {
    val subDirectoryPath = StoragePath + documentId
    new File(subDirectoryPath).mkdir()

    chunks.foldLeft(accumulator)((acc, chunkTpl) => {
      val (chunk, chunkNumber) = chunkTpl
      val filePath = s"$subDirectoryPath/$chunkNumber"
      Using.resource(new PrintWriter(new File(filePath)))(writer =>
        chunk
          .zip(Iterator from toAbsoluteLine(chunkNumber, 1))
          .foldLeft(acc)((subAcc, tpl) => {
            writer.write(tpl._1 :+ '\n')
            accFn(subAcc, tpl)
          })
       )
    })
  }
}
