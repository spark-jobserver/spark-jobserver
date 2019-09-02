package spark.jobserver.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream,
  ObjectInputStream, ObjectOutputStream, ObjectStreamClass}

import akka.serialization.JSerializer
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import spark.jobserver.JobManagerActor.StartJob
import org.slf4j.LoggerFactory
import spark.jobserver.io.BinaryInfo

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
      out.writeObject(startJob.mainClass)
      out.writeObject(startJob.cp)
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
    try {
      logger.debug(s"Deserializing StartJob object -- ${bytes.length}")
      val input = new ByteArrayInputStream(bytes)
      val inputStream = new ObjectInputStream(input) {
        // without this override serialization of non-primitive data types will
        // raise a java.lang.ClassNotFoundException because of the bug in Scala:
        // https://github.com/scala/bug/issues/9237
        override def resolveClass(aClass: ObjectStreamClass): Class[_] = {
          Class.forName(aClass.getName, false, this.getClass.getClassLoader)
        }
      }
      val mainClass = inputStream.readObject().asInstanceOf[String]
      val cp = inputStream.readObject().asInstanceOf[Seq[BinaryInfo]]
      val configString = inputStream.readObject().asInstanceOf[String]
      val subscribedEvents = inputStream.readObject().asInstanceOf[Set[Class[_]]]
      logger.debug(s"mainClass: ${mainClass}")
      logger.debug(s"cp: ${cp}")
      logger.debug(s"configString: ${configString}")
      logger.debug(s"subscribedEvents: ${subscribedEvents}")
      StartJob(mainClass, cp, ConfigFactory.parseString(configString), subscribedEvents)
    } catch {
      case ex: Exception =>
        throw new IllegalArgumentException(s"Object of unknown class cannot be deserialized " +
          s"${ex.getMessage}\n ${ex.getStackTrace}")
    }
  }
}