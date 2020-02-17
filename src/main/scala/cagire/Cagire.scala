package cagire

import java.io.{File, PrintWriter}
import scala.util.Try
import scala.util.hashing.MurmurHash3
import cats.implicits._
import io.circe.Json
import utils.FileUtils
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
    FileUtils.readFile(path)
      .map(_.sliding(ChunkSize, ChunkSize).zip(Iterator from 0))
      .map(fileIterator => {
        val head = fileIterator.next
        val documentId = MurmurHash3.orderedHash(head._1)
        val subDirectoryPath = StoragePath + documentId
        new File(subDirectoryPath).mkdir()

        (Iterator(head) ++ fileIterator).foldLeft(this)((acc, chunkTpl) => {
          val (chunk, chunkNumber) = chunkTpl
          val filePath = StoragePath + documentId + "/" + chunkNumber
          val writer = new PrintWriter(new File(filePath))
          val newCagire = chunk
            .zip(Iterator.from(ChunkSize * chunkNumber + 1))
            .foldLeft(acc)((cagire, tpl) => {
              writer.write(tpl._1 :+ '\n')
              cagire.ingestLine(documentId)(cagire, (tpl._2, tpl._1))
            })
          writer.close()
          newCagire
        }).copy(documentsIndex=this.documentsIndex.addDocument(documentId, path.split('/').last))
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
    _.map(matchTpl => {
      val (documentId, linesMatches) = matchTpl
      val filename = documentsIndex.getFilename(documentId)
      DocumentHandling
        .loadLinesFromDocument(documentId, linesMatches.toArray.toIndexedSeq)
        .map((filename -> _))
    }).toList.sequence.map(_.asJson)

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
