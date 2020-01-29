package cagire

import scala.util.Try
import io.circe.syntax._
import io.circe.parser.decode
import utils.FileUtils

final case class DocumentsIndex(index: Map[Int, String] = Map()) {

  def addDocument(documentId: Int, documentName: String): DocumentsIndex =
    this.copy(
      index=index + (documentId -> documentName)
    )

  def get: Int => String = index

  def commit(): Unit =
    FileUtils.writeFileAsync("documents_index.json", this.index.asJson.noSpaces)
}

object DocumentsIndex {

  def hydrate(): Try[DocumentsIndex] = {
    val documentsIndexFile = FileUtils.readFile("documents_index.json")
    decode[Map[Int, String]](documentsIndexFile)
      .toTry
      .map(DocumentsIndex(_))
  }
}
