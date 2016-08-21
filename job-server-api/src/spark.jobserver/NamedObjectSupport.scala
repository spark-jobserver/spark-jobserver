package spark.jobserver

import scala.concurrent.duration.FiniteDuration
trait NamedObject

/**
 * implementations of this abstract class should handle the specifics
 * of each named object's persistence
 */
abstract class NamedObjectPersister[O <: NamedObject] {

  /**
   * persists an object with the given name
   * @param namedObj - object to be persisted
   * @param name - name of the object
   */
  def persist(namedObj: O, name: String): Unit

  /**
   * update reference to named object so that it does not get GCed
   * @param namedObject - reference to this object is to be refreshed
   */
  def refresh(namedObject: O): O

  /**
   * unpersist the given object
   * @param namedObject - object to be unpersisted
   */
  def unpersist(namedObject: O): Unit
}

/**
 * NamedObjects - a trait that gives you safe, concurrent creation and access to named objects
 * such as RDDs or DataFrames
 * (the native SparkContext interface only has access to RDDs by numbers).
 * It facilitates easy sharing of data objects amongst jobs sharing the same SparkContext.
 * If two jobs simultaneously tries to create a data object with the same name and in the same namespace,
 * only one will win and the other will retrieve the same one.
 *
 * Note that to take advantage of NamedObjectSupport, a job must mix this in and use the APIs here instead of
 * the native DataFrame/RDD `cache()`, otherwise we will not know about the names.
 */
trait NamedObjects {

  def defaultTimeout : FiniteDuration

  /**
   * Gets a named object (NObj) with the given name, or creates it if one doesn't already exist.
   *
   * If the given NObj has already been computed by another job and cached in memory, this method will return
   * a reference to the cached NObj. If the NObj has never been computed, then the generator will be called
   * to compute it, in the caller's thread, and the result will be cached and returned to the caller.
   *
   * If an NObj is requested by thread B while thread A is generating the NObj, thread B will block up to
   * the duration specified by @timeout. If thread A finishes generating the NObj within that time, then
   * thread B will get a reference to the newly-created RDD. If thread A does not finish generating the
   * NObj within that time, then thread B will throw a timeout exception.
   *
   * @param name the unique name of the NObj. The uniqueness is scoped to the current SparkContext.
   * @param objGen a 0-ary function which will generate the NObj if it doesn't already exist.
   * @param timeout if the named object isn't created within this timeout, an error will be thrown.
   * @tparam O <: NamedObject the generic type of the named object.
   * @return the NObj with the given name.
   * @throws java.util.concurrent.TimeoutException if the request times out.
   * @throws java.lang.RuntimeException wrapping any error that occurs within the generator function.
   */
  def getOrElseCreate[O <: NamedObject](name: String, objGen: => O)
                                        (implicit timeout: FiniteDuration = defaultTimeout,
                                        persister: NamedObjectPersister[O]): O

  /**
   * Gets an named object (NObj) with the given name if it already exists and is cached.
   * If the NObj does not exist, None is returned.
   *
   * Note that a previously-known name object could 'disappear' if it hasn't been used for a while, because
   * for example, the SparkContext garbage-collects old cached RDDs.
   *
   * @param name the unique name of the NObj. The uniqueness is scoped to the current SparkContext.
   * @param timeout if the RddManager doesn't respond within this timeout, an error will be thrown.
   * @return the NObj with the given name.
   * @throws java.util.concurrent.TimeoutException if the request to the RddManager times out.
   */
  def get[O <: NamedObject](name: String)(implicit timeout: FiniteDuration = defaultTimeout): Option[O]

  /**
   * Replaces an existing named object (NObj) with a given name with a new object.
   * If an old named object for the given name existed,
   * it is un-persisted (non-blocking) and destroyed. It is safe to call this method when there is no
   * existing named object with the given name. If multiple threads call this around the same time,
   * the end result is undefined
   * - one of the generated RDDs will win and will be returned from future calls to get().
   *
   * The object generator function will be called from the caller's thread. Note that if this is called at the
   * same time as getOrElseCreate() for the same name, and completes before the getOrElseCreate() call,
   * then threads waiting for the result of getOrElseCreate() will unblock with the result of this
   * update() call. When the getOrElseCreate() succeeds, it will replace the result of this update() call.
   *
   * @param name the unique name of the name object. The uniqueness is scoped to the current SparkContext.
   * @param objGen a 0-ary function which will be called to generate the object in the caller's thread.
   * @tparam O <: NamedObject the generic type of the object.
   * @return the object with the given name.
   */
  def update[O <: NamedObject](name: String, objGen: => O)
                               (implicit timeout: FiniteDuration = defaultTimeout,
                               persister: NamedObjectPersister[O]): O

  /**
   * removes the named object with the given name, if one existed, from the cache
   * Has no effect if no named object with this name exists.
   *
   * The persister is not (!) asked to unpersist the object, use destroy instead if that is desired
   * @param name the unique name of the object. The uniqueness is scoped to the current SparkContext.
   */
  def forget(name: String): Unit

  /**
   * Destroys the named object with the given name, if one existed. The reference to the object
   * is removed from the cache and the persister is asked asynchronously to unpersist the
   * object iff it was found in the list of named objects.
   * Has no effect if no named object with this name is known to the cache.
   *
   * @param name the unique name of the object. The uniqueness is scoped to the current SparkContext.
   */
  def destroy[O <: NamedObject](objOfType: O, name: String)
                                (implicit persister: NamedObjectPersister[O]) : Unit

  /**
   * Returns the names of all named object that are managed by the named objects implementation.
   *
   * Note: this returns a snapshot of object names at one point in time. The caller should always expect
   * that the data returned from this method may be stale and incorrect.
   *
   * @return a collection of object names representing object managed by the NamedObjects implementation.
   */
  def getNames(): Iterable[String]
}

// NamedObjectSupport is not needed anymore due to JobEnvironment in api.SparkJobBase.  It is also
// imported into the old spark.jobserver.SparkJobBase automatically for compatibility.
@Deprecated
trait NamedObjectSupport