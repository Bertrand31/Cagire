package utils

import java.io.FileWriter
import scala.io.Source
import scala.util.{Try, Using}

object FileUtils {

  def readFile(path: String): Try[Iterator[String]] =
    Try {
      Source
        .fromResource(path)
        .getLines
    }

  def writeCSVProgressively(path: String, seq: => Iterator[_], chunkSize: Int = 10000): Unit =
    Using.resource(new FileWriter(path))(writer =>
      seq
        .sliding(chunkSize, chunkSize)
        .foreach((writer.write(_: String)) compose (_.mkString("\n") :+ '\n'))
    )
}
