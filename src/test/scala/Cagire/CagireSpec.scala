import cagire.Cagire
import org.scalatest.flatspec.AnyFlatSpec

class CagireSpec extends AnyFlatSpec {

  behavior of "The Cagire API"

  behavior of "the public API"

  private val cagire = Cagire.bootstrap()

  private val hydratedCagire = cagire.ingestFileHandler("t8.shakespeare.txt").get

  it should "ingest data" in {

    assert(hydratedCagire.numberOfDocuments === 1)
  }

  it should "fetch matches" in {

    val matches = hydratedCagire.searchPrefix("partake")(-383220880)
    assert(matches.getCardinality === 13)
  }
}
