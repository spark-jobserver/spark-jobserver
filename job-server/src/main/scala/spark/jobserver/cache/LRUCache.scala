package spark.jobserver.cache

import java.util
import java.util.Map.Entry

import com.typesafe.config.Config
import net.spy.memcached.compat.log.LoggerFactory

import scala.util.Try

/**
  * A convenience class to define a Least-Recently-Used Cache with a maximum size.
  * The oldest entries by time of last access will be removed when the number of entries exceeds
  * cacheSize.
  * For definitions of cacheSize and loadingFactor, see the docs for java.util.LinkedHashMap
  *
  * @see LinkedHashMap
  */
class LRUCache[K, V](cfg: Config) extends Cache[K, V] {
  private val logger = LoggerFactory.getLogger(getClass)
  private val cacheSize = Try(cfg.getInt("cache-size")).getOrElse(10000)
  private val loadingFactor = Try(cfg.getDouble("loading-factor")).getOrElse(0.75).toFloat

  logger.debug(s"Initilizing $getClass with Size: $cacheSize, Loading Factor: $loadingFactor")

  private val cache = {
    val initialCapacity = math.ceil(cacheSize / loadingFactor).toInt + 1
    new util.LinkedHashMap[K, V](initialCapacity, loadingFactor, true) {
      protected override def removeEldestEntry(p1: Entry[K, V]): Boolean = size() > cacheSize
    }
  }

  override def size: Int = cache.size()

  override def contains(k: K): Boolean = cache.get(k) != null

  override def get(k: K): V = cache.get(k)

  override def getOption(k: K): Option[V] = Option(this.get(k))

  override def getOrPut(k: K, v: => V): V = {
    logger.debug(s"Getting $k from cache...")
    cache.get(k) match {
      case null =>
        logger.debug(s"$k does not exist in cache, adding it...")
        cache.put(k, v)
        misses += 1
        v
      case vv =>
        hits += 1
        vv
    }
  }

  override def put(k: K, v: V): V = cache.put(k, v)
}

object LRUCache {
  def apply[K, V](cfg: Config): LRUCache[K, V] = new LRUCache[K, V](cfg)
}