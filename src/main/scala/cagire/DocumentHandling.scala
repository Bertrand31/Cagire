package cagire

import java.io.{File, PrintWriter}
import scala.annotation.tailrec
import scala.util.{Try, Using}
import scala.util.hashing.MurmurHash3
import scala.collection.mutable.PriorityQueue
import cats.implicits._
import utils.FileUtils

object DocumentHandling {

  def getSplitDocument(path: String): Try[(Int, Iterator[(Seq[String], Int)])] =
    FileUtils.readFile(path)
      .map(_.sliding(ChunkSize, ChunkSize).zipWithIndex)
      .map(chunksIterator => {
        val headChunk = chunksIterator.next
        val documentId = MurmurHash3.orderedHash(headChunk._1)
        val completeIterator = Iterator(headChunk) ++ chunksIterator
        (documentId -> completeIterator)
      })

  /** This method walks through an iterator of chunks, comitting them to the disk as well as
    * accumulating them onto an `accumulator`, using the `accFn` function.
    * The API and the code of this function are rather convoluted, but it allows us to both keep
    * the responsibilities split between the main Cagire case class and this file, as well as
    * having to walk through the file only once (and avoid any mutation).
    */
  def writeChunksAndCallback[A](
    documentId: Int,
    chunks: Iterator[(Seq[String], Int)],
    callback: ((String, Int)) => Unit,
  ): Unit = {
    val subDirectoryPath = StoragePath + "documents/" + documentId
    new File(subDirectoryPath).mkdirs()
    chunks.foreach(chunkTpl => {
      val (chunk, chunkNumber) = chunkTpl
      val filePath = s"$subDirectoryPath/$chunkNumber"
      Using.resource(new PrintWriter(new File(filePath)))(writer =>
        chunk
          .zip(Iterator from toAbsoluteLine(chunkNumber, 1))
          .foreach(tpl => {
            writer.write(tpl._1 :+ '\n')
            callback(tpl)
          })
       )
    })
  }

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

  def loadLinesFromDocument(documentId: Int, targets: Iterable[Int]): Try[Map[Int, String]] =
    targets
      .groupBy(toChunkNumber)
      .map({
        case (chunkNumber, lineNumbers) =>
          val absoluteLineNumbers = lineNumbers map toRelativeLine
          val targetsMinHeap = PriorityQueue(absoluteLineNumbers.toSeq:_*)(Ordering[Int].reverse)
          FileUtils.readFile(s"$StoragePath/documents/$documentId/$chunkNumber")
            .map(loadLines(targetsMinHeap))
            .map(_.map({ case (lineNb, line) => (toAbsoluteLine(chunkNumber, lineNb), line) }))
      })
      .toList
      .sequence
      .map(_ foldMap identity)
}
