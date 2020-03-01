package cagire

import scala.util.Try
import cats.implicits._
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.roaringbitmap.RoaringBitmap

class CagireController {

  var cagire = Cagire.bootstrap()

  private def ingestLine(docId: Int)(line: (String, Int)): Unit = {
    val (lineString, lineNumber) = line
    val words = LineSanitizing.lineToWords(lineString)
    val newTrie = cagire.indexesTrie.addLine(docId, lineNumber, words)
    this.cagire = cagire.copy(indexesTrie=newTrie)
  }

  def ingestFileHandler(path: String): Try[Unit] =
    DocumentHandling
      .getSplitDocument(path)
      .map(idAndChunks => {
        val (documentId, chunks) = idAndChunks
        DocumentHandling.writeChunksAndCallback(
          documentId,
          chunks,
          this.ingestLine(documentId),
        )
        val filename = path.split('/').last
        this.cagire = this.cagire.addDocument(documentId, filename)
      })

  def ingestFiles: List[String] => Try[Unit] =
    _
      .map(ingestFileHandler)
      .sequence
      .map(_ => { this.cagire = this.cagire.commitToDisk() })

  private def formatBasic: Map[Int, RoaringBitmap] => Json =
    _
      .map(_.bimap(cagire.getFilename, _.toArray))
      .asJson

  private def formatExtended: Map[Int, RoaringBitmap] => Try[Json] =
    _
      .map(matchTpl => {
        val (documentId, linesMatches) = matchTpl
        val filename = cagire.getFilename(documentId)
        DocumentHandling
          .loadLinesFromDocument(documentId, linesMatches.toArray)
          .map((filename -> _))
      })
      .toList
      .sequence
      .map(_.toMap.asJson)

  def searchWordAndGetMatches: String => Json =
    this.cagire.searchWord >>> formatBasic

  def searchPrefixAndGetMatches: String => Json =
    this.cagire.searchPrefix >>> formatBasic

  def searchWordAndGetLines: String => Try[Json] =
    this.cagire.searchWord >>> formatExtended

  def searchPrefixAndGetLines: String => Try[Json] =
    this.cagire.searchPrefix >>> formatExtended
}
