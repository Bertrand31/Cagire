package cagire

import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.io.Source
import scala.util.hashing.MurmurHash3.stringHash
import cats.implicits._

object FilesHandling {

  private def genFilename(path: String): Int = {
    val filename = path.split('/').last
    val timeStamp = System.currentTimeMillis
    val hash = stringHash(filename |+| timeStamp.toString)
    Math.abs(hash)
  }

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

  def loadDocumentWithLinesNumbers(documentId: Int): IndexedSeq[(Int, String)] = {
    val lines = loadDocument(documentId).toArray
    (1 to lines.length) zip lines
  }

}
