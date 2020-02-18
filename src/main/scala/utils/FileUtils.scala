package utils

import java.io.FileWriter
import scala.io.Source
import scala.util.Try

object FileUtils {

  def readFile(path: String): Try[Iterator[String]] =
    Try {
      Source
        .fromFile(path)
        .getLines
    }

  def writeCSVProgressively(path: String, seq: => Iterator[_], chunkSize: Int = 10000): Unit = {
    val fw = new FileWriter(path)
    seq
      .sliding(chunkSize, chunkSize)
      .foreach((fw.write(_: String)) compose (_.mkString("\n") :+ '\n'))
    fw.close
  }
}
