package utils

import java.io.{FileWriter, IOException}
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.nio.file.StandardOpenOption.{CREATE, WRITE}
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler}
import scala.io.Source
import scala.util.Try
import scala.concurrent.{ExecutionContext, Future, Promise}

object FileUtils {

  import ExecutionContext.Implicits.global

  def readFile(path: String): Try[Iterator[String]] =
    Try {
      Source
        .fromFile(path)
        .getLines
    }

  def writeCSVProgressively[A](path: String, seq: => Iterable[A], chunkSize: Int = 10000): Unit = {
    val fw = new FileWriter(path)
    seq
      .sliding(chunkSize, chunkSize)
      .foreach((fw.write(_: String)) compose (_.mkString("\n") + "\n"))
    fw.close
  }

  def writeAsync(file: String, bytes: Array[Byte]): Future[Unit] = {
    val p = Promise[Array[Byte]]()
    try {
      val channel = AsynchronousFileChannel.open(Paths.get(file), CREATE, WRITE)
      val buffer = ByteBuffer.wrap(bytes)
      channel.write(buffer, 0L, buffer, onComplete(channel, p))
    }
    catch {
      case t: Throwable => p.failure(t)
    }
    p.future.map(_ => ())
  }

  def writeFileAsync(file: String, s: String, charsetName: String = "UTF-8"): Future[Unit] =
    writeAsync(file, s.getBytes(charsetName))

  private def closeSafely(channel: AsynchronousFileChannel): Unit =
    try {
      channel.close()
    } catch {
      case _: IOException =>
    }

  private def onComplete(channel: AsynchronousFileChannel, p: Promise[Array[Byte]]): CompletionHandler[Integer, ByteBuffer] =
    new CompletionHandler[Integer, ByteBuffer]() {
      def completed(res: Integer, buffer: ByteBuffer): Unit = {
        p.complete(Try { buffer.array() })
        closeSafely(channel)
      }

      def failed(t: Throwable, buffer: ByteBuffer): Unit = {
        p.failure(t)
        closeSafely(channel)
      }
    }
}
