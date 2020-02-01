package cagire

import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.io.Source
import scala.util.hashing.MurmurHash3
import utils.FileUtils

object FilesHandling {

  private def genFilename(path: String): Int =
    Math.abs(MurmurHash3.orderedHash(FileUtils.readFile(path).get))

  private val StoragePath = "src/main/resources"

  def storeDocument(path: String): Int = {
    val filename = genFilename(path)
    Files.copy(
      Paths.get(path),
      Paths.get(s"$StoragePath/$filename"),
      StandardCopyOption.REPLACE_EXISTING,
    )
    filename
  }

  def loadDocument(documentId: Int): Iterator[String] =
    Source
      .fromFile(s"$StoragePath/${documentId.toString}")
      .getLines

  def loadDocumentWithLinesNumbers(documentId: Int): Stream[(Int, String)] =
    Stream.from(1) zip loadDocument(documentId).toIterable

  /** Loads the required lines from a lazy iterator without holding more than one line
    * in memory at any given point (except from the ones being accumulated).
    */
  private def loadLines(
    document: Iterator[String], targets: Set[Int], current: Int, soFar: Map[Int, String]
  ): Map[Int, String] =
    if (targets.isEmpty) soFar
    else {
      val head = document.next
      if (targets.contains(current)) {
        val newSoFar = soFar + (current -> head)
        if (!document.hasNext) newSoFar
        else loadLines(document, targets - current, current + 1, newSoFar)
      } else {
        if (!document.hasNext) soFar
        else loadLines(document, targets, current + 1, soFar)
      }
    }

    def loadLinesFromDocument(documentId: Int, targets: Array[Int]): Map[Int, String] =
      loadLines(loadDocument(documentId), targets.toSet, 1, Map())
}
