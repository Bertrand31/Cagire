package utils

import java.io.{BufferedWriter, File, FileWriter}

object FileUtils {

  private val StoragePath = "src/main/resources"

  def writeFile(filename: String, data: String): Unit = {
    val file = new File(StoragePath + "/" + filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(data)
    bw.close()
  }
}
