package cagire

import scala.util.Try
import akka.actor.{Actor, Props, PoisonPill}

object CagireFileHandler {

  def props = Props(new CagireFileHandler)

  case class Path(path: String)
  case class Line(documentId: Int, lineNumber: Int, words: Array[String])
  case class FileOver(documentId: Int, filename: String)
}

class CagireFileHandler extends Actor {

  import CagireFileHandler._

  private def ingestLine(docId: Int)(line: (String, Int)): Unit = {
    val (lineString, lineNumber) = line
    val words = LineSanitizing.lineToWords(lineString)
    sender ! Line(docId, lineNumber, words)
  }

  private def ingestFileHandler(path: String): Try[Unit] =
    DocumentHandling
      .getSplitDocument(path)
      .map(idAndChunks => {
        val (documentId, fileIterator) = idAndChunks
        val filename = path.split('/').last

        DocumentHandling.writeChunksAndCallback(documentId, fileIterator, ingestLine(documentId))

        sender ! FileOver(documentId, filename)
        self ! PoisonPill
      })

  def receive = {
    case Path(path) => ingestFileHandler(path)
  }
}
