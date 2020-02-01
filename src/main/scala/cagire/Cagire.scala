package cagire

import cats.implicits._
import utils.ArrayMonoid._
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

  private def ingestFileHandler(path: String): Cagire = {
    val documentId = FilesHandling.storeDocument(path)
    val documentLines = FilesHandling.loadDocumentWithLinesNumbers(documentId)
    val filename = path.split('/').last
    documentLines
      .foldLeft(this)(ingestLine(documentId))
      .copy(documentsIndex=this.documentsIndex.addDocument(documentId, filename))
  }

  def ingestFile: String => Cagire = ingestFileHandler(_).commitToDisk

  def ingestFiles: Iterable[String] => Cagire =
    _.foldLeft(this)(_ ingestFileHandler _).commitToDisk

  def searchWord: String => Map[Int, Array[Int]] = invertedIndex.searchWord

  def searchPrefix: String => Map[Int, Array[Int]] =
    indexesTrie
      .keysWithPrefix(_)
      .map(searchWord)
      .foldMap(identity)

  private def formatResults: Map[Int, Array[Int]] => Json =
    _.map(matchTpl => {
      val (documentId, linesMatches) = matchTpl
      val filename = documentsIndex.get(documentId)
      val matches = FilesHandling.loadLinesFromDocument(documentId, linesMatches)
      (filename -> matches)
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
