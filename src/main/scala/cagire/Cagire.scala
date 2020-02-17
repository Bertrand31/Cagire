package cagire

import scala.util.Try
import cats.implicits._
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.roaringbitmap.RoaringBitmap
import utils.TryUtils._

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

  private def commitToDisk(): Unit = {
    this.documentsIndex.commitToDisk()
    this.indexesTrie.commitToDisk()
  }

  private def ingestFileHandler(path: String): Try[Cagire] =
    DocumentHandling
      .getSplitDocument(path)
      .map(idAndChunks => {
        val (documentId, chunks) = idAndChunks
        val newCagire = DocumentHandling.writeChunks(
          documentId,
          chunks,
          this,
          this.ingestLine(documentId),
        )
        val filename = path.split('/').last
        newCagire.addDocument(documentId, filename)
      })

  def ingestFile: String => Try[Cagire] = ingestFileHandler(_).tap(_.commitToDisk)

  def ingestFiles: IterableOnce[String] => Try[Cagire] =
    _
      .iterator
      .foldLeft(Try(this))((acc, path) => acc.flatMap(_ ingestFileHandler path))
      .tap(_.commitToDisk)

  def searchWord: String => Map[Int, RoaringBitmap] = indexesTrie.matchesForWord

  def searchPrefix: String => Map[Int, RoaringBitmap] = indexesTrie.matchesWithPrefix

  private def formatResults: Map[Int, RoaringBitmap] => Try[Json] =
    _
      .map(matchTpl => {
        val (documentId, linesMatches) = matchTpl
        val filename = documentsIndex.getFilename(documentId)
        DocumentHandling
          .loadLinesFromDocument(documentId, linesMatches.toArray.toIndexedSeq)
          .map((filename -> _))
      })
      .to(LazyList)
      .sequence
      .map(_.asJson)

  def searchWordAndFormat: String => Try[Json] = searchWord >>> formatResults

  def searchPrefixAndFormat: String => Try[Json] = searchPrefix >>> formatResults
}

object Cagire {

  def bootstrap(): Cagire = {
    for {
      documentsIndex <- DocumentsIndex.hydrate
      indexesTrie <- IndexesTrie.hydrate
    } yield (Cagire(documentsIndex, indexesTrie))
  }.getOrElse(Cagire())
}
