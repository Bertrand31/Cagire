package cagire

import io.circe.syntax._
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
