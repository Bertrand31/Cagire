package cagire

import java.util.NoSuchElementException
import scala.util.{Success, Try}
import cats.implicits._
import org.roaringbitmap.RoaringBitmap
import io.circe.syntax.EncoderOps
import io.circe.parser.decode
import utils.FileUtils

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
    StoragePath ++ "inverted_index/" ++ _.toString ++ "-bucket.csv"

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
