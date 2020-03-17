package cagire

import cats.implicits._
import org.roaringbitmap.RoaringBitmap
import utils.RoaringBitmapMonoid.roaringBitmapMonoid

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
