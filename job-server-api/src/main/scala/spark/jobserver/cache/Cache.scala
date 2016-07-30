package spark.jobserver.cache

/**
  * Created by scarman on 7/29/16.
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
