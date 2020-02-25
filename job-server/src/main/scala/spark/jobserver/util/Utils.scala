package spark.jobserver.util

import java.io.{Closeable, File, PrintWriter, StringWriter}

import akka.actor.Address
import com.typesafe.config.Config
import com.yammer.metrics.core.Timer

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import org.slf4j.Logger
import spark.jobserver.JobServer

case class ErrorData(message: String, errorClass: String, stackTrace: String)

object ErrorData {
  def apply(ex: Throwable): ErrorData = {
    ErrorData(ex.getMessage, ex.getClass.getName, getStackTrace(ex))
  }

  def getStackTrace(ex: Throwable): String = {
    val stackTrace = new StringWriter()
    ex.printStackTrace(new PrintWriter(stackTrace))
    stackTrace.toString
  }
}

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

  def timedFuture[T](timer: Timer)(future: Future[T])
      (implicit executor: ExecutionContext): Future[T] = {
    val tc = timer.time()
    future.andThen({
      case _ => tc.stop()
    })
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

  def getSeqFromConfig(config: Config, key: String): Seq[String] = {
    Try(config.getStringList(key).asScala).
      orElse(Try(config.getString(key).split(",").toSeq)).getOrElse(Nil)
  }

  def getHASeedNodes(config: Config): List[Address] = {
    config.hasPath("spark.jobserver.ha.masters") match {
      case true =>
        config.getStringList("spark.jobserver.ha.masters")
          .asScala
          .toList
          .map(extractHostAndPort)
          .map { node =>
            Address("akka.tcp", JobServer.ACTOR_SYSTEM_NAME, node.host, node.port)
          }
      case false => List.empty
    }
  }

  private def extractHostAndPort(address: String): MasterAddress = {
    val hostAndPort = address.split(':')
    hostAndPort.length match {
      case 2 => MasterAddress(hostAndPort(0), hostAndPort(1).toInt)
      case _ => throw new WrongFormatException(
        s"Address should be of format HOST:PORT, wrong format specified $address")
    }
  }
}

final case class MasterAddress(host: String, port: Int)
