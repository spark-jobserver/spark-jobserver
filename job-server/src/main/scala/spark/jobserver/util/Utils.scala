package spark.jobserver.util

import java.io.{Closeable, File}
import com.yammer.metrics.core.{Stoppable, Timer}

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

  def usingTimer[B](timer: Timer)(f: () => B): B = {
    val tc = timer.time()
    try {
        f()
    } finally {
       tc.stop()
    }
  }
}
