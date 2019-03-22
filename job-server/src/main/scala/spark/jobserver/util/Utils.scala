package spark.jobserver.util

import java.io.{Closeable, File}
import java.net.{URI, URISyntaxException}

object Utils {
  def usingResource[A <: Closeable, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }

  /**
    * Return a well-formed URI for the file described by a user input string.
    *
    * If the supplied path does not contain a scheme, or is a relative path, it will be
    * converted into an absolute path with a file:// scheme.
    */
  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
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
}
