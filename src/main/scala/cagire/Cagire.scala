package cagire

import scala.util.Try
import cats.implicits._
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.roaringbitmap.RoaringBitmap

final case class Cagire(
  private val documentsIndex: DocumentsIndex = DocumentsIndex(),
  private val indexesTrie: IndexesTrie = IndexesTrie(),
  private val dirtyPrefixes: Set[String] = Set(),
) {

  private def ingestLine(docId: Int)(cagire: Cagire, line: (String, Int)): Cagire = {
    val (lineString, lineNumber) = line
    val words = LineSanitizing.lineToWords(lineString)
    val newDirtyPrefixes = this.dirtyPrefixes ++ words.map(_.take(IndexesTrie.PrefixLength))
    val newTrie = cagire.indexesTrie.addLine(docId, lineNumber, words)
    cagire.copy(indexesTrie=newTrie, dirtyPrefixes=newDirtyPrefixes)
  }

  private def addDocument(documentId: Int, filename: String): Cagire =
    this.copy(documentsIndex=this.documentsIndex.addDocument(documentId, filename))

  private def commitToDisk(): Cagire = {
    this.documentsIndex.commitToDisk()
    this.indexesTrie.commitToDisk(dirtyPrefixes)
    this.copy(dirtyPrefixes=dirtyPrefixes.empty)
  }

  private def ingestFileHandler(path: String): Try[Cagire] =
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

  def ingestFile: String => Try[Cagire] = ingestFileHandler(_).map(_.commitToDisk)

  def ingestFiles: IterableOnce[String] => Try[Cagire] =
    _
      .iterator
      .foldLeft(Try(this))((acc, path) => acc.flatMap(_ ingestFileHandler path))
      .map(_.commitToDisk)

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
      .map(_.toMap.asJson)

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
