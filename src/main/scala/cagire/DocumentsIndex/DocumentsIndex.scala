package cagire

import scala.util.Try
import utils.FileUtils

final case class DocumentsIndex(index: Map[Int, String] = Map()) {

  import DocumentsIndex.DocumentsIndexFilePath

  def addDocument(documentId: Int, documentName: String): DocumentsIndex =
    this.copy(this.index + (documentId -> documentName))

  def get: Int => String = index

  def commitToDisk(): Unit =
    FileUtils.writeCSVProgressively(
      DocumentsIndexFilePath,
      this.index.view.map({ case (id, filename) => id.toInt + ";" + filename }),
    )
}

object DocumentsIndex {

  private val DocumentsIndexFilePath = StoragePath + "documents_index.csv"

  def hydrate(): Try[DocumentsIndex] = {
    FileUtils
      .readFile(DocumentsIndexFilePath)
      .map(
        _.map(line => {
          val Array(id, filename) = line.split(';')
          (id.toInt -> filename)
        }).toMap
      )
      .map(DocumentsIndex(_))
  }
}
