package cagire

import scala.util.Try
import org.roaringbitmap.RoaringBitmap

final case class Cagire(
  val documentsIndex: DocumentsIndex = DocumentsIndex(),
  val indexesTrie: IndexesTrie = IndexesTrie(),
) {

  def addDocument(documentId: Int, filename: String): Cagire =
    this.copy(documentsIndex=this.documentsIndex.addDocument(documentId, filename))

  def commitToDisk(): Cagire =
    this.copy(
      documentsIndex=this.documentsIndex.commitToDisk(),
      indexesTrie=this.indexesTrie.commitToDisk(),
    )

  def getFilename: Int => String = documentsIndex.getFilename

  def searchWord: String => Map[Int, RoaringBitmap] = indexesTrie.matchesForWord

  def searchPrefix: String => Map[Int, RoaringBitmap] = indexesTrie.matchesWithPrefix
}

object Cagire {

  def bootstrap(): Cagire = {
    for {
      documentsIndex <- DocumentsIndex.hydrate
      indexesTrie = IndexesTrie.hydrate
    } yield (Cagire(documentsIndex, indexesTrie))
  }.getOrElse(Cagire())
}
