package cagire

import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import cats.implicits._
import org.roaringbitmap.RoaringBitmap
import utils.RoaringBitmapMonoid.roaringBitmapMonoid

final case class Cagire(
  private val documentsIndex: DocumentsIndex = DocumentsIndex(),
  private val indexesTrie: IndexesTrie = IndexesTrie(),
) {

  private def ingestLine(docId: Int)(cagire: Cagire, line: (String, Int)): Cagire = {
    val (lineString, lineNumber) = line
    val words = LineSanitizing.lineToWords(lineString)
    val newTrie = cagire.indexesTrie.addLine(docId, lineNumber, words)
    cagire.copy(indexesTrie=newTrie)
  }

  private def addDocument(documentId: Int, filename: String): Cagire =
    this.copy(documentsIndex=this.documentsIndex.addDocument(documentId, filename))

  def commitToDisk(): Cagire =
    Cagire(
      documentsIndex=this.documentsIndex.commitToDisk(),
      indexesTrie=this.indexesTrie.commitToDisk(),
    )

  def ingestFileHandler(path: String): Try[Cagire] =
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

  def getFilename: Int => String = documentsIndex.getFilename

  def searchWordsOr: String => Map[Int, RoaringBitmap] =
    _.split(' ').toVector match {
      case words :+ prefix => words.foldMap(indexesTrie.matchesForWord) |+| searchPrefix(prefix)
      case _               => Map()
    }

  def searchWordsAnd: String => Map[Int, RoaringBitmap] =
    _.split(' ').toVector match {
      case words :+ prefix =>
        val wordsMatches = words.map(indexesTrie.matchesForWord)
        val prefixMatches = searchPrefix(prefix)
        wordsMatches
          .foldLeft(prefixMatches.view)((acc, matches) => {
            val keys = matches.keys.toSet
            acc filterKeys keys.contains
          })
          .map({
            case (key, bitmap) =>
              val newBitMap = wordsMatches.foldLeft(bitmap)((acc, item) =>
                acc.tap(_ and item(key))
              )
              (key, newBitMap)
          })
          .filterNot({ case (_, bitmap) => bitmap.isEmpty })
          .toMap
      case _ => Map()
    }

  def searchPrefix: String => Map[Int, RoaringBitmap] = indexesTrie.matchesWithPrefix
}

object Cagire {

  def bootstrap(): Cagire = {
    for {
      documentsIndex <- DocumentsIndex.hydrate()
      indexesTrie    <- IndexesTrie.hydrate()
    } yield (Cagire(documentsIndex, indexesTrie))
  }.getOrElse(Cagire())
}
