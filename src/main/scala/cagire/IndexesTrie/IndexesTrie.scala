package cagire

import java.util.NoSuchElementException
import scala.util.{Success, Try}
import cats.implicits._
import org.roaringbitmap.RoaringBitmap
import io.circe.syntax.EncoderOps
import io.circe.parser.decode
import utils.FileUtils
import utils.RoaringBitmapMonoid.roaringBitmapMonoid

final case class IndexesTrie(
  private val trie: IndexesTrieNode = IndexesTrieNode(),
  private val dirtyBuckets: Set[Int] = Set.empty,
) {

  import IndexesTrie._

  // We'll use the hash of the first 2 letters of all
  // words to determine which bucket they'll be stored in
  private val BucketPrefixLength = 2

  private def getBucket: String => Int = _.hashCode % IndexBucketsNumber

  def isEmpty: Boolean = this.trie.isEmpty

  def addLine(docId: Int, lineNumber: Int, words: Array[String]): IndexesTrie =
    IndexesTrie(
      trie=this.trie.addLine(docId, lineNumber, words),
      dirtyBuckets=(this.dirtyBuckets ++ words.map(_ take BucketPrefixLength).map(getBucket)),
    )

  private def getPrefixTuples(
    prefixLength: Int,
    current: IndexesTrieNode,
    prefix: String = "",
  ): Map[String, Map[Int, RoaringBitmap]] =
    current
      .children
      .flatMap {
        case (char, subTrie) =>
          val newPrefix = prefix :+ char
          if (prefixLength === 1) subTrie.getTuples(newPrefix)
          else getPrefixTuples(prefixLength - 1, subTrie, newPrefix)
      }

  def commitToDisk(): IndexesTrie = {
    getPrefixTuples(BucketPrefixLength, this.trie)
      .filter({ case (prefix, _) => dirtyBuckets.contains(getBucket(prefix)) })
      .groupBy({ case (prefix, _) => getBucket(prefix) })
      .foreach({
        case (bucket, matches) =>
          FileUtils.writeCSVProgressively(
            getFilePathFromBucket(bucket),
            matches.iterator.map({
              case (prefix, matches) =>
                val matchesStr = matches.view.mapValues(_.toArray).toMap.asJson.noSpaces
                s"$prefix;$matchesStr"
            }),
          )
      })
    this.copy(dirtyBuckets=dirtyBuckets.empty)
  }

  def matchesWithPrefix: String => Map[Int, RoaringBitmap] = this.trie.matchesWithPrefix

  def matchesForWord: String => Map[Int, RoaringBitmap] = this.trie.matchesForWord
}

object IndexesTrie {

  private def decodeIndexLines: Iterator[String] => Iterator[(String, Map[Int, RoaringBitmap])] =
    _
      .map(line => {
        val Array(word, matchesStr) = line.split(';')
        val matches =
          decode[Map[Int, Array[Int]]](matchesStr)
            .getOrElse(Map.empty)
            .view
            .mapValues(RoaringBitmap.bitmapOf(_:_*))
            .toMap
        (word -> matches)
      })

  private def getFilePathFromBucket: Int => String =
    StoragePath |+| "inverted_index/" |+| _.toString |+| "-bucket.csv"

  private val IndexBucketsNumber = 1000

  def hydrate(): Try[IndexesTrie] = {
    val indexFiles =
      (0 until IndexBucketsNumber)
        .map(getFilePathFromBucket >>> FileUtils.readFile)
        .collect({ case Success(lines) => decodeIndexLines(lines) })
    Either.cond(
      !indexFiles.isEmpty,
      IndexesTrie(
        indexFiles.foldLeft(IndexesTrieNode())((acc, lines) => lines.foldLeft(acc)(_ insertTuple _))
      ),
      new NoSuchElementException("Could not read the inverted index from the disk"),
    ).toTry
  }
}

/* This implementation of a Trie uses Maps instead of Arrays to store the child nodes.
 * This approach is slower to explore, but since we want this IndexesTrie to support
 * all 16-bits characters of a Scala string, it was either Maps or Arrays of
 * length 65535, which would have blown up memory use.
 */
final case class IndexesTrieNode(
  val children: Map[Char, IndexesTrieNode] = Map.empty,
  private val matches: Map[Int, RoaringBitmap] = Map.empty,
) {

  def isEmpty: Boolean = this.matches.isEmpty && this.children.isEmpty

  private def getIndexesFromString: String => Seq[Char] =
    _
      .toLowerCase
      .toCharArray
      .map(_.charValue) // 'a' is 97, 'b' is 98, etc
      .toList

  def addLine(docId: Int, lineNumber: Int, words: Array[String]): IndexesTrieNode =
    words.foldLeft(this)((acc, word) => {
      def insertIndexes(indexes: Seq[Char], trie: IndexesTrieNode): IndexesTrieNode =
        indexes match {
          case head +: Nil => {
            val child = trie.children.getOrElse(head, IndexesTrieNode())
            val currentMatches = child.matches.get(docId).getOrElse(new RoaringBitmap)
            currentMatches add lineNumber
            val newChildMatches = child.matches + (docId -> currentMatches)
            val newChild = child.copy(matches=newChildMatches)
            val newChildren = trie.children + (head -> newChild)
            trie.copy(children=newChildren)
          }
          case head +: tail => {
            val newSubTrie = trie.children.get(head) match {
              case Some(subTrie) => insertIndexes(tail, subTrie)
              case None => insertIndexes(tail, IndexesTrieNode())
            }
            trie.copy(trie.children + (head -> newSubTrie))
          }
        }

      insertIndexes(getIndexesFromString(word), acc)
    })

  private def getAllMatches: Map[Int, RoaringBitmap] =
    this.children
      .map(_._2.getAllMatches)
      .foldLeft(this.matches)(_ |+| _)

  private def getSubTrie(path: Seq[Char], trie: IndexesTrieNode): Option[IndexesTrieNode] =
    path match {
      case head +: Nil  => trie.children.get(head)
      case head +: tail => trie.children.get(head).flatMap(getSubTrie(tail, _))
    }

  def matchesWithPrefix(prefix: String): Map[Int, RoaringBitmap] =
    getSubTrie(getIndexesFromString(prefix), this)
      .fold(Map.empty[Int, RoaringBitmap])(_.getAllMatches)

  def matchesForWord(word: String): Map[Int, RoaringBitmap] =
    getSubTrie(getIndexesFromString(word), this)
      .fold(Map.empty[Int, RoaringBitmap])(_.matches)

  def insertTuple(wordAndMatches: (String, Map[Int, RoaringBitmap])): IndexesTrieNode = {
    val (word, matches) = wordAndMatches
    val path = getIndexesFromString(word)

    def insertMatches(indexes: Seq[Char], trie: IndexesTrieNode): IndexesTrieNode =
      indexes match {
        case head +: Nil =>
          val child = trie.children.getOrElse(head, IndexesTrieNode())
          val newChild = child.copy(matches=matches)
          val newChildren = trie.children + (head -> newChild)
          trie.copy(children=newChildren)
        case head +: tail =>
          val newSubTrie = trie.children.get(head) match {
            case Some(subTrie) => insertMatches(tail, subTrie)
            case None => insertMatches(tail, IndexesTrieNode())
          }
          trie.copy(trie.children + (head -> newSubTrie))
      }

    insertMatches(path, this)
  }

  def getTuples(prefix: String): Iterator[(String, Map[Int, RoaringBitmap])] = {
    val subTuples = this.children.to(Iterator).flatMap({
      case (char, subTrie) => subTrie.getTuples(prefix :+ char)
    })
    if (this.matches.isEmpty) subTuples
    else Iterator((prefix, this.matches)) ++ subTuples
  }
}
