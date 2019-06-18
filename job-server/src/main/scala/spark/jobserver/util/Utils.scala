package spark.jobserver.util

import java.io.{Closeable, File, StringWriter, PrintWriter}
import scala.util.{Failure, Success, Try}
import org.slf4j.Logger

object Utils {
  def usingResource[A <: Closeable, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }

  def createDirectory(folderPath: String): Unit = {
    val folder = new File(folderPath)
    createDirectory(folder)
  }

  def createDirectory(folder: File): Unit = {
    if (!folder.exists()) {
      if (!folder.mkdirs()) {
        throw new RuntimeException(s"Could not create directory $folder")
      }
    }
  }

  def retry[T](n: Int, retryDelayInMs: Int = 500)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case _ if n > 1 =>
        Thread.sleep(retryDelayInMs)
        retry(n - 1)(fn)
      case Failure(e) => throw e
    }
  }

  def logStackTrace(logger: Logger, exception: Throwable): Unit = {
    val sw = new StringWriter
    exception.printStackTrace(new PrintWriter(sw))
    logger.error(s"${exception.getMessage} : ${sw.toString}")
  }

}
