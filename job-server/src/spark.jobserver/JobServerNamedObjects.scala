package spark.jobserver

import akka.actor.ActorSystem
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try
import org.slf4j.LoggerFactory
import spray.caching.{ LruCache, Cache }
import spray.util._

/**
 * An implementation of [[NamedObjects]] API for the Job Server.
 * Note that this contains code that executes on the same thread as the job.
 * Uses spray caching for cache references to named objects and to
 * avoid that the same object is created multiple times
 */
class JobServerNamedObjects(system: ActorSystem) extends NamedObjects {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val ec: ExecutionContext = system.dispatcher

  val config = system.settings.config

  // Default timeout is 60 seconds. Hopefully that is enough
  // to let most RDD/DataFrame generator functions finish.
  val defaultTimeout = Timeout(Duration(
                Try(config.getInt("spark.jobserver.named-object-creation-timeout")).getOrElse(60),
                                         java.util.concurrent.TimeUnit.SECONDS))

  // we must store a reference to each NamedObject even though only its ID is used here
  // this reference prevents the object from being GCed and cleaned by sparks ContextCleaner
  // or some other GC for other types of objects
  private val namesToObjects: Cache[NamedObject] = LruCache()

  override def getOrElseCreate[O <: NamedObject](name: String, objGen: => O)
                                 (implicit timeoutOpt: Option[Timeout] = None,
                                           persister: NamedObjectPersister[O]): O = {
    implicit val timeout = getTimeout(timeoutOpt)
    cachedOp(name, createObject(objGen, name)).await(timeout) match {
      case obj: O @unchecked =>
        logger.info("Named object [{}] of type [{}] created", name, obj.getClass)
        obj
      case NamedRDD(_, _, _)    => throw new IllegalArgumentException("Incorrect type for named object")
    }
  }

  // we can wrap the operation with caching support
  // (providing a caching key)
  private def cachedOp[O <: NamedObject](name: String, f: () => O): Future[NamedObject] =
    namesToObjects(name) {
       logger.info("Named object [{}] not found, starting creation", name)
       f()
  }

  private def createObject[O <: NamedObject](objGen: => O, name: String)
                  (implicit persister: NamedObjectPersister[O]): () => O = {
    () =>
      {
        val namedObj: O = objGen
        persister.saveToContext(namedObj, name)
        namedObj
      }
  }

  override def get[O <: NamedObject](name: String)(implicit timeoutOpt: Option[Timeout] = None): Option[O] = {
    implicit val timeout = getTimeout(timeoutOpt)
    //namesToObjects.get:  retrieves the future instance that is currently in the cache for the given key.
    // Returns None if the key has no corresponding cache entry.
    namesToObjects.get(name) match {
      case Some(f: Future[O]) =>
        Some(f.await(timeout))
        //TODO - refresh?
      case None =>
        //appears that we have never seen this named object before or that it was removed
        None
    }
  }

  //TODO - isn't this identical to getOrElseCreate?
  // only that the object may already exist and we need to remove it - unless the old
  // object (rdd) has the same id TODO - is that even possible? The API does not
  // mention that special case either
  override def update[O <: NamedObject](name: String, objGen: => O)
                                        (implicit timeoutOpt: Option[Timeout] = None,
                                            persister: NamedObjectPersister[O]): O = {
    get(name) match {
      case Some(namedObject) =>
        destroy(name)
      case _ =>
    }
    getOrElseCreate(name, objGen)
  }

  override def destroy(name: String) {
    namesToObjects.remove(name)
    //TODO do we need some call to the persister?
    // was: rdd.unpersist(blocking = false)
  }

  override def getNames(): Iterable[String] = {
    namesToObjects.keys match {
      case answer: Iterable[String] @unchecked => answer
    }
  }

  private def getTimeout(timeoutOpt: Option[Timeout]) : Timeout = {
    timeoutOpt match {
      case Some(t) => t
      case _ => defaultTimeout
    }
  }
}
