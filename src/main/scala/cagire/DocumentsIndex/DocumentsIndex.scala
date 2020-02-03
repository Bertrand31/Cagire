package cagire

import scala.util.Try
import scala.concurrent.Future
import io.circe.syntax.EncoderOps
import io.circe.parser.decode
import utils.FileUtils

final case class DocumentsIndex(index: Map[Int, String] = Map()) {

  import DocumentsIndex._

  def addDocument(documentId: Int, documentName: String): DocumentsIndex =
    this.copy(this.index + (documentId -> documentName))

  def get: Int => String = index

  def commitToDisk(): Future[Unit] =
    FileUtils.writeFileAsync(DocumentsIndexFilePath, this.index.asJson.noSpaces)
}

object DocumentsIndex {

  private val DocumentsIndexFilePath = StoragePath + "/documents_index.json"

  private def decodeFile: String => Try[Map[Int, String]] =
    decode[Map[Int, String]](_).toTry

  def hydrate(): Try[DocumentsIndex] = {
    FileUtils
      .readFile(DocumentsIndexFilePath)
      .map(_.mkString)
      .flatMap(decodeFile)
      .map(DocumentsIndex(_))
  }
}
