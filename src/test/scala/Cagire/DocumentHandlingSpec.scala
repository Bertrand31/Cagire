import scala.collection.mutable.PriorityQueue
import cagire.DocumentHandling
import org.scalatest.flatspec.AnyFlatSpec

class DocumentHandlingSpec extends AnyFlatSpec {

  behavior of "The DocumentHandling helper methods"

  behavior of "the loadLinesFromDocument method"

  val fakeDocument = Iterator.from(1).map(_.toString).take(20)

  it should "store a document name and ID" in {

    val targetsMinHeap = PriorityQueue(1, 3, 8)(Ordering[Int].reverse)
    val expectedOutput = Map(
      (1 -> "1"),
      (3 -> "3"),
      (8 -> "8"),
    )
    assert(DocumentHandling.loadLines(targetsMinHeap)(fakeDocument) == expectedOutput)
  }
}
