package cagire

import cats.implicits._
import utils.ArrayMonoid._

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

  def ingestFile(path: String): Cagire = {
    val documentId = FilesHandling.storeDocument(path)
    val documentLines = FilesHandling.loadDocumentWithLinesNumbers(documentId)
    val filename = path.split('/').last
    val updatedCagire = documentLines
      .foldLeft(this)(ingestLine(documentId))
      .copy(documentsIndex=this.documentsIndex.addDocument(documentId, filename))
    updatedCagire.documentsIndex.commitToDisk()
    updatedCagire.invertedIndex.commitToDisk()
    updatedCagire
  }

  def ingestFiles: Iterable[String] => Cagire = _.foldLeft(this)(_ ingestFile _)

  def searchWord: String => Map[Int, Array[Int]] = invertedIndex.searchWord

  def searchPrefix: String => Map[Int, Array[Int]] =
    indexesTrie
      .keysWithPrefix(_)
      .map(searchWord)
      .foldMap(identity)

  type Output = Map[String, Array[String]]

  private def formatResults: Map[Int, Array[Int]] => Output =
    _.map(matchTpl => {
      val (documentId, linesMatches) = matchTpl
      val filename = documentsIndex.get(documentId)
      val lines = FilesHandling.loadDocument(documentId).take(linesMatches.max).toArray
      val matches = linesMatches
        .distinct
        .map(line => s"$line: ${lines(line - 1)}")
      (filename, matches)
    }).toMap

  def searchWordAndFormat: String => Output = formatResults compose searchWord

  def searchPrefixAndShow: String => Output = formatResults compose searchPrefix
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
