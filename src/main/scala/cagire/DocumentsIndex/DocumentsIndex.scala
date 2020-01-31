package cagire

import scala.util.Try
import scala.concurrent.Future
import io.circe.syntax.EncoderOps
import io.circe.parser.decode
import utils.FileUtils

final case class DocumentsIndex(index: Map[Int, String] = Map()) {

  def addDocument(documentId: Int, documentName: String): DocumentsIndex =
    this.copy(
      index=index + (documentId -> documentName)
    )

  def get: Int => String = index

  def commitToDisk(): Future[Unit] =
    FileUtils.writeFileAsync("documents_index.json", this.index.asJson.noSpaces)
}

object DocumentsIndex {

  private val DocumentsIndexFileName = "documents_index.json"

  private def decodeFile: String => Try[Map[Int, String]] =
    decode[Map[Int, String]](_).toTry

  def hydrate(): Try[DocumentsIndex] = {
    FileUtils
      .readFile(DocumentsIndexFileName)
      .flatMap(decodeFile)
      .map(DocumentsIndex(_))
  }
}
