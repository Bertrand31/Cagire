package cagire

import scala.util.Try
import cats.implicits._
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.roaringbitmap.RoaringBitmap
import akka.actor.{Actor, Props}

object CagireHandler {

  def props: Props = Props(new CagireHandler)

  case class IngestPaths(paths: List[String])
}

class CagireHandler extends Actor {

  import CagireFileHandler._
  import CagireHandler.IngestPaths

  private def createFileHandler = context.actorOf(CagireFileHandler.props)

  private var cagire = Cagire.bootstrap()

  private def handleLine(docId: Int, lineNumber: Int, words: Array[String]): Unit = {
    val newTrie = this.cagire.indexesTrie.addLine(docId, lineNumber, words)
    this.cagire = this.cagire.copy(indexesTrie=newTrie)
  }

  private def ingestFiles: List[String] => Unit =
    _
      .foreach(path => {
        val fileHandler = createFileHandler
        fileHandler ! CagireFileHandler.Path(path)
      })

  def receive = {
    case IngestPaths(paths) => ingestFiles(paths)
    case Line(documentId, lineNumber, words) => handleLine(documentId, lineNumber, words)
    case FileOver(documentId, filename) =>
      this.cagire =
        this.cagire
          .addDocument(documentId, filename)
          .commitToDisk()
  }

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
