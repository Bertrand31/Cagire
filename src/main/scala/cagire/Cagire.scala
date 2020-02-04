package cagire

import scala.util.Try
import cats.implicits._
import io.circe.syntax._
import io.circe.Json

final case class Cagire(
  private val documentsIndex: DocumentsIndex = DocumentsIndex(),
  private val invertedIndex: InvertedIndex = InvertedIndex(),
  private val indexesTrie: IndexesTrie = IndexesTrie(),
) {

  private def ingestLine(docId: Int)(cagire: Cagire, line: (Int, String)): Cagire = {
    val (lineNumber, lineString) = line
    val words = LineSanitizing.lineToWords(lineString)
    val newTrie = cagire.indexesTrie ++ words
    val newIndex = cagire.invertedIndex.addLine(docId, lineNumber, words)
    cagire.copy(invertedIndex=newIndex, indexesTrie=newTrie)
  }

  private def commitToDisk(): Cagire = {
    this.documentsIndex.commitToDisk()
    this.invertedIndex.commitToDisk()
    this
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

  def ingestFile: String => Try[Cagire] = ingestFileHandler(_).map(_.commitToDisk)

  def ingestFiles: Iterable[String] => Try[Cagire] =
    _
      .foldLeft(Try(this))((acc, path) => acc.flatMap(_ ingestFileHandler path))
      .map(_.commitToDisk)

  def searchWord: String => Map[Int, Set[Int]] = invertedIndex.searchWord

  def searchPrefix: String => Map[Int, Set[Int]] =
    indexesTrie
      .keysWithPrefix(_)
      .map(searchWord)
      .foldMap(identity)

  private def formatResults: Map[Int, Set[Int]] => Json =
    _.flatMap(matchTpl => {
      val (documentId, linesMatches) = matchTpl
      val filename = documentsIndex.get(documentId)
      DocumentHandling.loadLinesFromDocument(documentId, linesMatches)
        .toOption
        .map((filename -> _))
    }).asJson

  def searchWordAndFormat: String => Json = formatResults compose searchWord

  def searchPrefixAndShow: String => Json = formatResults compose searchPrefix
}

object Cagire {

  def bootstrap(): Cagire = {
    for {
      documentsIndex <- DocumentsIndex.hydrate
      invertedIndex <- InvertedIndex.hydrate
      indexesTrie = IndexesTrie(invertedIndex.keys:_*)
    } yield (Cagire(documentsIndex, invertedIndex, indexesTrie))
  }.getOrElse(Cagire())
}
