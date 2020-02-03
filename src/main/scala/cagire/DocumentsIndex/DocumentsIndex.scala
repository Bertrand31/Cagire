package cagire

import java.io.FileWriter
import scala.util.Try
import utils.FileUtils

final case class DocumentsIndex(index: Map[Int, String] = Map()) {

  import DocumentsIndex.DocumentsIndexFilePath

  def addDocument(documentId: Int, documentName: String): DocumentsIndex =
    this.copy(this.index + (documentId -> documentName))

  def get: Int => String = index

  private val ChunkSize = 10000

  def commitToDisk(): Unit = {
    val fw = new FileWriter(DocumentsIndexFilePath)
    this.index
      .sliding(ChunkSize, ChunkSize)
      .foreach(chunk => {
        fw.write {
          chunk
            .map({ case (id, filename) => id.toInt + ";" + filename })
            .mkString("\n") + "\n"
        }
    })
    fw.close
  }
}

object DocumentsIndex {

  private val DocumentsIndexFilePath = StoragePath + "/documents_index.csv"

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
