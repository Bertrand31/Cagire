package cagire

import java.io.{File, PrintWriter}
import scala.util.{Try, Using}
import cats.implicits._
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.roaringbitmap.RoaringBitmap
import utils.TryUtils._

final case class Cagire(
  private val documentsIndex: DocumentsIndex = DocumentsIndex(),
  private val indexesTrie: IndexesTrie = IndexesTrie(),
) {

  private def ingestLine(docId: Int)(line: (String, Int)): Cagire = {
    val (lineString, lineNumber) = line
    val words = LineSanitizing.lineToWords(lineString)
    val newTrie = this.indexesTrie.addLine(docId, lineNumber, words)
    this.copy(indexesTrie=newTrie)
  }

  private def commitToDisk(): Unit = {
    this.documentsIndex.commitToDisk()
    this.indexesTrie.commitToDisk()
  }

  private def ingestFileHandler(path: String): Try[Cagire] =
    DocumentHandling.getSplitDocument(path).map(tpl => {
      val (documentId, chunks) = tpl
      val subDirectoryPath = StoragePath + documentId
      new File(subDirectoryPath).mkdir()

      chunks.foldLeft(this)((acc, chunkTpl) => {
        val (chunk, chunkNumber) = chunkTpl
        val filePath = s"$StoragePath$documentId/$chunkNumber"
        Using.resource(new PrintWriter(new File(filePath)))(writer =>
          chunk
            .zip(Iterator.from(ChunkSize * chunkNumber + 1))
            .foldLeft(acc)((cagire, tpl) => {
              writer.write(tpl._1 :+ '\n')
              cagire.ingestLine(documentId)(tpl)
            })
        )
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
