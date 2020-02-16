import cagire.DocumentsIndex

import org.scalatest.flatspec.AnyFlatSpec

class DocumentsIndexSpec extends AnyFlatSpec {

  behavior of "The DocumentsIndex implementation"

  val newIndex = DocumentsIndex()

  behavior of "the addDocument method"

  val withDocument = newIndex.addDocument(123, "foo.txt")

  it should "store a document name and ID" in {

    assert(withDocument.index == Map((123 -> "foo.txt")))
  }

  behavior of "the getFilename method"

  it should "retrieve the filename for a given ID" in {

    assert(withDocument.getFilename(123) == "foo.txt")
  }
}
