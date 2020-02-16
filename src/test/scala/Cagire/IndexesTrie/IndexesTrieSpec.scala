import cagire.IndexesTrie
import org.roaringbitmap.RoaringBitmap

import org.scalatest.flatspec.AnyFlatSpec

class IndexesTrieSpec extends AnyFlatSpec {

  behavior of "The IndexesTrie implementation"

  behavior of "the apply method"

  val newTrie = IndexesTrie()

  it should "create an empty IndexesTrie" in {

    assert(newTrie.isEmpty)
  }

  behavior of "the addLine method"

  val words = Array("this", "is", "a", "sample", "sentence")
  val newTrieWithLine = newTrie.addLine(123, 1, words)

  it should "ingest a line" in {

    assert(!newTrieWithLine.isEmpty)
  }

  behavior of "the matchesForWord method"

  it should "get exact matches for a given word that has been inserted previously" in {

    val matches = newTrieWithLine.matchesForWord("sentence")
    assert(matches == Map((123 -> RoaringBitmap.bitmapOf(1))))
  }

  val words2 = Array("Ceci", "est", "une", "seconde", "ligne")
  val words3 = Array("Hello", "world")
  val trieWithTwoLines =
    newTrieWithLine
      .addLine(123, 2, words2)
      .addLine(1234, 1, words3)

  behavior of "the matchesWithPrefix method"

  it should "get matches for words starting with the given prefix within inserted words" in {

    val matches = trieWithTwoLines.matchesWithPrefix("se")
    assert(matches == Map((123 -> RoaringBitmap.bitmapOf(1, 2))))
  }
}
