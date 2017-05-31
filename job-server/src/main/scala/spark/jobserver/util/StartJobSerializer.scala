package spark.jobserver.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import akka.serialization.JSerializer
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import spark.jobserver.JobManagerActor.StartJob

import org.slf4j.LoggerFactory

/**
  * Akka Serialization extension for StartJob message to accommodate large config values (>64KB)
  */
class StartJobSerializer extends JSerializer {
  val logger = LoggerFactory.getLogger(getClass)

  override def includeManifest() : Boolean = false

  override def identifier() : Int = 12376

  override def toBinary(obj: AnyRef): Array[Byte] = {
    logger.debug(s"Serializing StartJob object -- ${obj}")
    try {
      val byteArray = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(byteArray)
      val startJob = obj.asInstanceOf[StartJob]
      out.writeObject(startJob.appName)
      out.writeObject(startJob.classPath)
      out.writeObject(startJob.config.root().render(ConfigRenderOptions.concise()))
      out.writeObject(startJob.subscribedEvents)
      out.flush()
      byteArray.toByteArray
    } catch {
      case ex : Exception =>
        throw new IllegalArgumentException(s"Object of unknown class cannot be serialized " +
          s"${ex.getMessage}")
    }
  }

  override def fromBinaryJava(bytes: Array[Byte],
                              clazz: Class[_]): AnyRef = {
    logger.debug(s"Deserializing StartJob object -- ${bytes.length}")
    try {
      val input = new ByteArrayInputStream(bytes)
      val inputStream = new ObjectInputStream(input)
      val appName = inputStream.readObject().asInstanceOf[String]
      val classPath = inputStream.readObject().asInstanceOf[String]
      val configString = inputStream.readObject().asInstanceOf[String]
      val subscribedEvents = inputStream.readObject().asInstanceOf[Set[Class[_]]]
      logger.debug(s"appname: ${appName}")
      logger.debug(s"classPath: ${classPath}")
      logger.debug(s"configString: ${configString}")
      logger.debug(s"subscribedEvents: ${subscribedEvents}")
      StartJob(appName, classPath, ConfigFactory.parseString(configString), subscribedEvents)
    } catch {
      case ex: Exception =>
        throw new IllegalArgumentException(s"Object of unknown class cannot be deserialized " +
          s"${ex.getMessage}")
    }
  }
}