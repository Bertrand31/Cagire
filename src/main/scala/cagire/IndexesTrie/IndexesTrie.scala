package cagire

/* This implementation of a IndexesTrie uses Maps instead of Arrays to store the child nodes.
 * This approach is slower to explore, but since we want this IndexesTrie to support
 * all 16-bits characters of a Scala string, it was either Maps or Arrays of
 * length 65535, which would have blown up memory use.
 */

final case class IndexesTrie(
  private val children: Map[Char, IndexesTrie] = Map(),
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

  def +(word: String): IndexesTrie = {
    def insertIndexes(indexes: Seq[Char], trie: IndexesTrie): IndexesTrie =
      indexes match {
        case head +: Nil => {
          val newChild = trie.children.getOrElse(head, IndexesTrie()).copy(isFinal=true)
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

    insertIndexes(getIndexesFromString(word), this)
  }

  def ++(words: Iterable[String]): IndexesTrie = words.foldLeft(this)(_ + _)

  def keys: List[String] = {
    def descendCharByChar(accumulator: Vector[Char], trie: IndexesTrie): List[Vector[Char]] =
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

    def descendWithPrefix(indexes: Seq[Char], trie: IndexesTrie): Option[IndexesTrie] =
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
}

object IndexesTrie {

  def apply(initialItems: String*): IndexesTrie = new IndexesTrie ++ initialItems
}
