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

  private def ingestLine(docId: Int)(cagire: Cagire, line: (Int, String)): Cagire = {
    val (lineNumber, lineString) = line
    val words = LineSanitizing.lineToWords(lineString)
    val newTrie = cagire.indexesTrie.addLine(docId, lineNumber, words)
    cagire.copy(indexesTrie=newTrie)
  }

  private def commitToDisk(): Unit = {
    this.documentsIndex.commitToDisk()
    this.indexesTrie.commitToDisk()
  }

  private def ingestFileHandler(path: String): Try[Cagire] =
    for {
      documentId <- DocumentHandling.storeDocument(path)
      documentLines <- DocumentHandling.loadDocumentWithLinesNumbers(documentId)
    } yield {
      val filename = path.split('/').last
      documentLines
        .foldLeft(this)(ingestLine(documentId))
        .copy(documentsIndex=this.documentsIndex.addDocument(documentId, filename))
    }

  def ingestFile: String => Try[Cagire] = ingestFileHandler(_).tap(_.commitToDisk)

  def ingestFiles: Iterable[String] => Try[Cagire] =
    _
      .foldLeft(Try(this))((acc, path) => acc.flatMap(_ ingestFileHandler path))
      .tap(_.commitToDisk)

  def searchWord: String => Map[Int, RoaringBitmap] = indexesTrie.matchesForWord

  def searchPrefix: String => Map[Int, RoaringBitmap] = indexesTrie.matchesWithPrefix

  private def formatResults: Map[Int, RoaringBitmap] => Json =
    _.map(matchTpl => {
      val (documentId, linesMatches) = matchTpl
      val filename = documentsIndex.getFilename(documentId)
      val matches =
        DocumentHandling
          .loadLinesFromDocument(documentId, linesMatches.toArray.toIndexedSeq)
          .getOrElse(Map())
      (filename -> matches)
    }).asJson

  def searchWordAndFormat: String => Json = searchWord >>> formatResults

  def searchPrefixAndFormat: String => Json = searchPrefix >>> formatResults
}

object Cagire {

  def bootstrap(): Cagire = {
    for {
      documentsIndex <- DocumentsIndex.hydrate
      indexesTrie <- IndexesTrie.hydrate
    } yield (Cagire(documentsIndex, indexesTrie))
  }.getOrElse(Cagire())
}
