package cagire

import scala.collection.immutable.ArraySeq
import cats.implicits._
import utils.ArraySeqMonoid._

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

  def ingestFiles: IterableOnce[String] => Cagire = _.iterator.foldLeft(this)(_ ingestFile _)

  def searchWord: String => Map[Int, ArraySeq[Int]] = invertedIndex.searchWord

  def searchPrefix: String => Map[Int, ArraySeq[Int]] =
    indexesTrie
      .keysWithPrefix(_)
      .map(searchWord)
      .foldMap(identity)

  import Console._

  private def printResults: Map[Int, ArraySeq[Int]] => Unit =
    _.foreach(matchTpl => {
      val (documentId, linesMatches) = matchTpl
      val filename = documentsIndex.get(documentId)
      val lines = FilesHandling.loadDocument(documentId).take(linesMatches.max).toArray
      println(s"\n${GREEN}${BOLD}$filename:${RESET}")
      linesMatches
        .distinct
        .foreach(line => println(s"${YELLOW}$line${RESET}: ${lines(line - 1)}"))
    })

  def searchAndShow: String => Unit = printResults compose searchWord

  def searchPrefixAndShow: String => Unit = printResults compose searchPrefix
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
