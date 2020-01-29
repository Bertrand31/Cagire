package cagire

object Main extends App {

  for {
    documentsIndex <- DocumentsIndex.hydrate
    invertedIndex <- InvertedIndex.hydrate
    indexesTrie = IndexesTrie(invertedIndex.keys:_*)
  } {
    val cagire = Cagire(documentsIndex, invertedIndex, indexesTrie)
    cagire.searchPrefixAndShow("sim")
  }
}
