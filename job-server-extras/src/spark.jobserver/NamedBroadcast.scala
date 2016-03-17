package spark.jobserver

import org.apache.spark.broadcast.Broadcast

/**
 * wrapper for named objects of type Broadcast
 */
case class NamedBroadcast[T](broadcast: Broadcast[T]) extends NamedObject

/**
  * implementation of a NamedObjectPersister for Broadcast objects
  */
class BroadcastPersister[T] extends NamedObjectPersister[NamedBroadcast[T]] {
  override def persist(namedObj: NamedBroadcast[T], name: String) {
  }
  override def unpersist(namedObj: NamedBroadcast[T]) {
    namedObj match {
      case NamedBroadcast(broadcast) =>
        broadcast.unpersist(blocking = false)
    }
  }
  /**
    * @param namedBroadcast the NamedBroadcast to refresh
    */
  override def refresh(namedBroadcast: NamedBroadcast[T]): NamedBroadcast[T] = namedBroadcast match {
    case NamedBroadcast(broadcast) =>
      namedBroadcast
  }
}