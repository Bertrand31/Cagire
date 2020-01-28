package cagire

final case class DocumentsIndex(index: Map[Int, String] = Map()) {

  def addDocument(documentId: Int, documentName: String): DocumentsIndex =
    this.copy(
      index=index + (documentId -> documentName)
    )

  def get: Int => String = index
}
