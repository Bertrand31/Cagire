package utils

import java.io.{BufferedWriter, File, FileWriter}
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler}
import java.nio.file.Paths
import java.nio.file.StandardOpenOption._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

object FileUtils {

  import ExecutionContext.Implicits.global

  private val StoragePath = "src/main/resources"

  def writeFile(filename: String, data: String): Unit = {
    val file = new File(StoragePath + "/" + filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(data)
    bw.close()
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
    p.future.map(_ => {})
  }

  def writeFileAsync(file: String, s: String, charsetName: String = "UTF-8"): Future[Unit] =
    writeAsync(file, s.getBytes(charsetName))

  private def closeSafely(channel: AsynchronousFileChannel) =
    try {
      channel.close()
    } catch {
      case _: IOException => // TODO: Handle
    }

  private def onComplete(channel: AsynchronousFileChannel, p: Promise[Array[Byte]]) = {
    new CompletionHandler[Integer, ByteBuffer]() {
      def completed(res: Integer, buffer: ByteBuffer): Unit = {
        p.complete(Try {
          buffer.array()
        })
        closeSafely(channel)
      }

      def failed(t: Throwable, buffer: ByteBuffer): Unit = {
        p.failure(t)
        closeSafely(channel)
      }
    }
  }
}
