package cagire

object Main extends App {

  for {
    documentsIndex <- DocumentsIndex.hydrate
    invertedIndex <- InvertedIndex.hydrate
  } {
    val indexesTrie = IndexesTrie() ++ invertedIndex.keys
    val cagire = Cagire(documentsIndex, invertedIndex, indexesTrie)
    cagire.searchPrefixAndShow("sim")
  }
}
