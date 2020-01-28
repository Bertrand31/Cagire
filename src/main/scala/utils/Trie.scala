package utils

/* This implementation of a Trie uses Maps instead of Arrays to store the child nodes.
 * This approach is slower to explore, but since we want this Trie to support
 * all 16-bits characters of a Scala string, it was either Maps or Arrays of
 * length 65535, which would have blown up memory use.
 */

final case class Trie(
  private val children: Map[Char, Trie] = Map(),
  private val isFinal: Boolean = false,
) {

  private def getIndexesFromString: String => Seq[Char] =
    _
      .toLowerCase
      .toCharArray
      .map(_.charValue) // 'a' is 97, 'b' is 98, etc
      .toList

  private def getStringFromIndexes: Seq[Char] => String =
    _.mkString("")

  def +(word: String): Trie = {
    def insertIndexes(indexes: Seq[Char], trie: Trie): Trie =
      indexes match {
        case head +: Nil => {
          val newChild = trie.children.getOrElse(head, Trie()).copy(isFinal=true)
          val newChildren = trie.children + (head -> newChild)
          trie.copy(children=newChildren)
        }
        case head +: tail => {
          val newSubTrie = trie.children.get(head) match {
            case Some(subTrie) => insertIndexes(tail, subTrie)
            case None => insertIndexes(tail, Trie())
          }
          trie.copy(trie.children + (head -> newSubTrie))
        }
      }

    insertIndexes(getIndexesFromString(word), this)
  }

  def ++(words: IterableOnce[String]): Trie = words.iterator.foldLeft(this)(_ + _)

  def ++(trie: Trie): Trie = this ++ trie.keys

  def contains(word: String): Boolean = {
    def endsOnLastIndex(indexes: Seq[Char], trie: Trie): Boolean =
      indexes match {
        case head +: Nil => trie.children.get(head).map(_.isFinal).getOrElse(false)
        case head +: tail => trie.children.get(head).map(endsOnLastIndex(tail, _)).getOrElse(false)
      }

    endsOnLastIndex(getIndexesFromString(word), this)
  }

  def keys(): List[String] = {
    def descendCharByChar(accumulator: Vector[Char], trie: Trie): List[Vector[Char]] =
      trie.children.map({
        case (char, subTrie) if (subTrie.isFinal) => {
          val currentWord = accumulator :+ char
          currentWord +: descendCharByChar(currentWord, subTrie)
        }
        case (char, subTrie) => descendCharByChar(accumulator :+ char, subTrie)
      }).flatten.toList

    descendCharByChar(Vector(), this).map(getStringFromIndexes)
  }

  def keysWithPrefix(prefix: String): List[String] = {

    def descendWithPrefix(indexes: Seq[Char], trie: Trie): Option[Trie] =
      indexes match {
        case head +: Nil => trie.children.get(head)
        case head +: tail => trie.children.get(head).flatMap(descendWithPrefix(tail, _))
      }

    val subTrie = descendWithPrefix(getIndexesFromString(prefix), this)
    subTrie match {
      case None => List()
      case Some(subTrie) if (subTrie.isFinal) => prefix +: subTrie.keys.map(prefix + _)
      case Some(subTrie) => subTrie.keys.map(prefix + _)
    }
  }

  lazy val isEmpty: Boolean = this.children.isEmpty
}

object Trie {

  def apply(initialItems: String*): Trie = new Trie ++ initialItems
}
