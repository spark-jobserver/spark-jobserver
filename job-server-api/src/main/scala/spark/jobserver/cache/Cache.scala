package spark.jobserver.cache

/**
  * This is the trait for implementing a cache interface in the SJS
  *
  * @tparam K the key type
  * @tparam V the value type
  * @see spark.jobserver.cache.LRUCache
  * @see spark.jobserver.cache.RedisCache
  */
trait Cache[K, V] {

  protected var hits = 0
  protected var misses = 0

  def size: Int

  def get(k: K): V

  def getOrPut(k: K, v: => V): V

  def put(k: K, v: V): V

  def contains(k: K): Boolean

  def getOption(k: K): Option[V]

  def hitRatio: Double = misses / math.max(hits + misses, 1)
}
