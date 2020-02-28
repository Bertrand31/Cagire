package cagire

import scala.util.{Failure, Success}
import cats.implicits._
import org.roaringbitmap.RoaringBitmap
import io.circe.syntax.EncoderOps
import io.circe.parser.decode
import utils.FileUtils
import utils.RoaringBitmapMonoid.roaringBitmapMonoid

/* This implementation of a IndexesTrie uses Maps instead of Arrays to store the child nodes.
 * This approach is slower to explore, but since we want this IndexesTrie to support
 * all 16-bits characters of a Scala string, it was either Maps or Arrays of
 * length 65535, which would have blown up memory use.
 */

final case class IndexesTrie(
  private val children: Map[Char, IndexesTrie] = Map(),
  private val matches: Map[Int, RoaringBitmap] = Map(),
) {

  import IndexesTrie._

  private def getIndexesFromString: String => Seq[Char] =
    _
      .toLowerCase
      .toCharArray
      .map(_.charValue) // 'a' is 97, 'b' is 98, etc
      .toList

  def addLine(docId: Int, lineNumber: Int, words: Array[String]): IndexesTrie =
    words.foldLeft(this)((acc, word) => {
      def insertIndexes(indexes: Seq[Char], trie: IndexesTrie): IndexesTrie =
        indexes match {
          case head +: Nil => {
            val child = trie.children.getOrElse(head, IndexesTrie())
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
              case None => insertIndexes(tail, IndexesTrie())
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

  private def getSubTrie(path: Seq[Char], trie: IndexesTrie): Option[IndexesTrie] =
    path match {
      case head +: Nil => trie.children.get(head)
      case head +: tail => trie.children.get(head).flatMap(getSubTrie(tail, _))
    }

  def matchesWithPrefix(prefix: String): Map[Int, RoaringBitmap] =
    getSubTrie(getIndexesFromString(prefix), this)
      .fold(Map[Int, RoaringBitmap]())(_.getAllMatches)

  def matchesForWord(word: String): Map[Int, RoaringBitmap] =
    getSubTrie(getIndexesFromString(word), this)
      .fold(Map[Int, RoaringBitmap]())(_.matches)

  def insertTuple(wordAndMatches: (String, Map[Int, RoaringBitmap])): IndexesTrie = {
    val (word, matches) = wordAndMatches
    val path = getIndexesFromString(word)

    def insertMatches(indexes: Seq[Char], trie: IndexesTrie): IndexesTrie =
      indexes match {
        case head +: Nil => {
          val child = trie.children.getOrElse(head, IndexesTrie())
          val newChild = child.copy(matches=matches)
          val newChildren = trie.children + (head -> newChild)
          trie.copy(children=newChildren)
        }
        case head +: tail => {
          val newSubTrie = trie.children.get(head) match {
            case Some(subTrie) => insertMatches(tail, subTrie)
            case None => insertMatches(tail, IndexesTrie())
          }
          trie.copy(trie.children + (head -> newSubTrie))
        }
      }

    insertMatches(path, this)
  }

  private def getTuples(prefix: String): Iterator[(String, Map[Int, RoaringBitmap])] = {
    val subTuples = this.children.view.to(Iterator).flatMap({
      case (char, subTrie) => subTrie.getTuples(prefix :+ char)
    })
    if (this.matches.isEmpty) subTuples
    else Iterator((prefix, this.matches)) ++ subTuples
  }

  def isEmpty: Boolean = this.matches.isEmpty && this.children.isEmpty

  private def getBucket: String => Int = _.hashCode % IndexBucketsNumber

  def commitToDisk(dirtyPrefixes: Set[String]): Unit = {
    this.children
      .flatMap({
        case (char, subTrie) =>
          subTrie.children.map({
            case (subChar, subTrie) => (s"$char$subChar", subTrie)
          })
      })
      .filter({ case (prefix, _) => dirtyPrefixes.contains(prefix) })
      .groupBy({ case (prefix, _) => getBucket(prefix) })
      .foreach({
        case (bucket, branches) =>
          FileUtils.writeCSVProgressively(
            makeBucketFilename(bucket),
            branches.iterator.flatMap({
              case (prefix, subTrie) =>
                subTrie
                  .getTuples(prefix)
                  .map({ case (word, matches) =>
                    s"$word;${matches.view.mapValues(_.toArray).toMap.asJson.noSpaces}"
                  })
            }),
          )
      })
  }
}

object IndexesTrie {

  private def makeBucketFilename: Int => String =
    StoragePath + "inverted_index/" + _ + "-bucket.csv"

  private val IndexBucketsNumber = 200

  val PrefixLength = 2

  private def decodeIndexLines: Iterator[String] => Iterator[(String, Map[Int, RoaringBitmap])] =
    _
      .map(line => {
        val Array(word, matchesStr) = line.split(';')
        val matches =
          decode[Map[Int, Array[Int]]](matchesStr)
            .getOrElse(Map())
            .view
            .mapValues(RoaringBitmap.bitmapOf(_:_*))
            .toMap
        (word -> matches)
      })


  def hydrate(): IndexesTrie =
    (0 until IndexBucketsNumber)
      .foldLeft(IndexesTrie())((trie, bucket) =>
        FileUtils.readFile(makeBucketFilename(bucket)) match {
          case Success(lines) => decodeIndexLines(lines).foldLeft(trie)(_ insertTuple _)
          case Failure(_) => trie
        }
      )
}
