package cagire

import scala.util.Try
import cats.implicits._
import utils.FileUtils

final case class DocumentsIndex(index: Map[Int, String] = Map.empty) {

  import DocumentsIndex.DocumentsIndexFilePath

  def addDocument(documentId: Int, documentName: String): DocumentsIndex =
    this.copy(this.index + (documentId -> documentName))

  def getFilename: Int => String = index

  def commitToDisk(): DocumentsIndex = {
    FileUtils.writeCSVProgressively(
      DocumentsIndexFilePath,
      this.index
        .view
        .to(Iterator)
        .map({ case (id, filename) => s"$id;$filename" }),
    )
    this
  }
}

object DocumentsIndex {

  private val DocumentsIndexFilePath = StoragePath |+| "documents_index.csv"

  def hydrate(): Try[DocumentsIndex] =
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
