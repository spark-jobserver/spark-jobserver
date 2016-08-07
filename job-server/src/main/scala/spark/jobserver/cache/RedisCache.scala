package spark.jobserver.cache

import com.redis._
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.Config
import org.apache.commons.lang.SerializationUtils

import scala.util.Try

class RedisCache[V <: Serializable](config: Config) extends Cache[String, V] {

  private val host: String = Try(config.getString("host")).getOrElse("localhost")
  private val port: Int = Try(config.getInt("port")).getOrElse(8080)
  private val client: RedisClientPool = new RedisClientPool(host, port)

  private[cache] def valueToBytes(v: V): Array[Byte] = {
    SerializationUtils.serialize(v)
  }

  private[cache] def bytesToValue(v: Array[Byte]): V = {
    SerializationUtils.deserialize(v).asInstanceOf[V]
  }

  override def size: Int =
    client.withClient[Long](c => c.dbsize.getOrElse(0)).toInt

  override def get(k: String): V = {
    val byteArray = client.withClient[Array[Byte]] { c =>
      c.get[Array[Byte]](k).orNull
    }
    hits += 1
    bytesToValue(byteArray)
  }

  override def getOrPut(k: String, v: => V): V = {
    val byteArray = client.withClient[Array[Byte]] { c =>
      c.getset[Array[Byte]](k, valueToBytes(v)).orNull
    }
    if (byteArray == null) {
      misses += 1
      v
    } else {
      hits += 1
      v
    }
  }

  override def put(k: String, v: V): V = {
    val result = client.withClient[Boolean] { c =>
      c.set(k, valueToBytes(v))
    }
    if (result) { v } else {
      throw RedisMultiExecException(s"Cannot place key: $k in $host:$port")
    }
  }

  override def contains(k: String): Boolean = {
    client.withClient[Boolean](c => c.exists(k))
  }

  override def getOption(k: String): Option[V] = {
    client.withClient[Option[V]]{c =>
      val bytes = c.get[Array[Byte]](k)
      bytes.map(b => bytesToValue(b))
    }
  }
}
