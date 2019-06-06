package spark.jobserver.util

import java.io.{Closeable, File}
import scala.util.{Failure, Success, Try}

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
}
