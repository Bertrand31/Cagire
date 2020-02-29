package cagire

import scala.util.Try
import org.roaringbitmap.RoaringBitmap

final case class Cagire(
  private val documentsIndex: DocumentsIndex = DocumentsIndex(),
  private val indexesTrie: IndexesTrie = IndexesTrie(),
) {

  private def ingestLine(docId: Int)(cagire: Cagire, line: (String, Int)): Cagire = {
    val (lineString, lineNumber) = line
    val words = LineSanitizing.lineToWords(lineString)
    val newTrie = cagire.indexesTrie.addLine(docId, lineNumber, words)
    cagire.copy(indexesTrie=newTrie)
  }

  private def addDocument(documentId: Int, filename: String): Cagire =
    this.copy(documentsIndex=this.documentsIndex.addDocument(documentId, filename))

  def commitToDisk(): Cagire = {
    this.documentsIndex.commitToDisk()
    val newIndexesTrie = this.indexesTrie.commitToDisk()
    this.copy(indexesTrie=newIndexesTrie)
  }

  def ingestFileHandler(path: String): Try[Cagire] =
    DocumentHandling
      .getSplitDocument(path)
      .map(idAndChunks => {
        val (documentId, chunks) = idAndChunks
        val newCagire = DocumentHandling.writeChunksAndAccumulate(
          documentId,
          chunks,
          this,
          this.ingestLine(documentId),
        )
        val filename = path.split('/').last
        newCagire.addDocument(documentId, filename)
      })

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
