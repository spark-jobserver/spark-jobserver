package spark.jobserver

import akka.actor.{ ActorRef, ActorSystem }
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import org.apache.spark.SparkContext
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

  // we must store a reference to each NamedObject even though only its ID is used here
  // this reference prevents the object from being GCed and cleaned by sparks ContextCleaner
  // or some other GC for other types of objects
  private val namesToObjects: Cache[NamedObject] = LruCache()

  override def getOrElseCreate[O <: NamedObject](name: String, objGen: => O)
                                (implicit timeout: Timeout = defaultTimeout,
                                    persister: NamedObjectPersister[O]): O = {

    val res : Option[O] = getExistingObject(name)
    res match {
      case Some(namedObject) =>
        namedObject
      case None =>
        logger.info("Named object [{}] not found, starting creation", name)
        val f = cachedOp(name, createObject(objGen, name))
        f.await match {
          case obj : O => obj
          case NamedRDD(_, _) => throw new IllegalArgumentException("Incorrect type for named object")
        }
    }
  }

  // we can wrap the operation with caching support
  // (providing a caching key)
  private def cachedOp[O <: NamedObject](name: String, f: () => O):
                           Future[NamedObject] = namesToObjects(name) {
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

  private def getExistingObject[O](name: String): Option[O] = {
    //get:  retrieves the future instance that is currently in the cache for the given key.
    // Returns None if the key has no corresponding cache entry.
    namesToObjects.get(name) match {
      case Some(f : Future[O]) =>
        Some(f.await)
        //TODO - the implementation for NamedRDDs checked here as follows. Do
        // we still need to do this?
      //        sparkContext.getPersistentRDDs.get(rdd.id) match {
      //          case Some(rdd) => Some(rdd)
      //          case None =>
      //            // If this happens, maybe we never knew about this RDD,
      // or maybe we had a name -> id mapping, but
      //            // spark's MetadataCleaner has evicted this RDD from
      //the cache because it was too old, and we need
      //            // to forget about it. Remove it from our names -> ids map
      //            // and respond as if we never knew about it.
      //            namesToRDDs.remove(name)
      //            None
      //        }
      case None =>
        //appears that we have never seen this named object before or that it was removed
        None
    }
  }
  override def get(name: String)(implicit timeout: Timeout = defaultTimeout): Option[NamedObject] = {
    getExistingObject(name) match {
      case objectOpt: Option[NamedObject] @unchecked => objectOpt //TODO  rddOpt.map { rdd=> refresh(rdd) }
    }
  }

  //TODO - isn't this identical to getOrElseCreate?
  // only that the object may already exist and we need to remove it - unless the old
  // object (rdd) has the same id TODO - is that even possible? The API does not
  // mention that special case either
  override def update[O <: NamedObject](name: String,
                                        objGen: => O)(implicit persister: NamedObjectPersister[O]): O = {
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

  override def getNames()(implicit timeout: Timeout = defaultTimeout): Iterable[String] = {
    namesToObjects.keys match {
      case answer: Iterable[String] @unchecked => answer
    }
  }
}
