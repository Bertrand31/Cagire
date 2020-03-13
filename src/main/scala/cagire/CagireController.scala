package cagire

import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import cats.implicits._
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.roaringbitmap.RoaringBitmap

class CagireController {

  private var cagire = Cagire.bootstrap()

  def ingestFiles: IterableOnce[String] => Try[Cagire] =
    _
      .iterator
      .foldLeft(Try(this.cagire))((acc, path) => acc.flatMap(_ ingestFileHandler path))
      .map(_.commitToDisk)
      .tap(_.map { this.cagire = _ })

  def formatBasic: Map[Int, RoaringBitmap] => Json =
    _
      .map(_.bimap(cagire.getFilename, _.toArray))
      .asJson

  def formatExtended: Map[Int, RoaringBitmap] => Try[Json] =
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

  def searchWordsWithAnd: String => Map[Int, RoaringBitmap] = this.cagire.searchWordsAnd

  def searchWordsWithOr: String => Map[Int, RoaringBitmap] = this.cagire.searchWordsOr
}
