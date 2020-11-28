package spark.jobserver

import akka.actor.ActorSystem
import akka.http.caching.LfuCache
import akka.http.caching.scaladsl.{Cache, CachingSettings}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import org.slf4j.LoggerFactory

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
  val defaultTimeout = FiniteDuration(
    config.getDuration("spark.jobserver.named-object-creation-timeout", SECONDS), SECONDS)

  // we must store a reference to each NamedObject even though only its ID is used here
  // this reference prevents the object from being GCed and cleaned by sparks ContextCleaner
  // or some other GC for other types of objects
  val defaultCachingSettings = CachingSettings(system)
  private val namesToObjects: Cache[String, NamedObject] = LfuCache(defaultCachingSettings)

  override def getOrElseCreate[O <: NamedObject](name: String, objGen: => O)
                                 (implicit timeout: FiniteDuration = defaultTimeout,
                                           persister: NamedObjectPersister[O]): O = {
    val obj = Await.result(cachedOp(name, createObject(objGen, name)), timeout).asInstanceOf[O]
    logger.info(s"Named object [$name] of type [${obj.getClass.toString}] created")
    obj
  }

  // wrap the operation with caching support
  // (providing a caching key)
  private def cachedOp[O <: NamedObject](name: String, f: () => O): Future[NamedObject] =
    namesToObjects.getOrLoad(name, _ => {
      logger.info("Named object [{}] not found, starting creation", name)
      val future = Future { f() }
      future.onComplete{
        case Success(_) =>
          logger.debug("Named object [{}] created", name)
        case Failure(exception) =>
          logger.error("Named object [{}] creation failed with error: {}",
            name, exception.toString, exception)
      }
      future
    })

  private def createObject[O <: NamedObject](objGen: => O, name: String)
                  (implicit persister: NamedObjectPersister[O]): () => O = {
    () =>
      {
        val namedObj: O = objGen
        persister.persist(namedObj, name)
        namedObj
      }
  }

  override def get[O <: NamedObject](name: String)
                (implicit timeout : FiniteDuration = defaultTimeout): Option[O] = {
    namesToObjects.get(name).map(f => Await.result(f, timeout).asInstanceOf[O])
  }

  override def update[O <: NamedObject](name: String, objGen: => O)
                                        (implicit timeout : FiniteDuration = defaultTimeout,
                                            persister: NamedObjectPersister[O]): O = {
    get(name) match {
      case None => {}
      case Some(_) => forget(name)
    }
    //this does not work when the old object is not of the same type as the new one
    //  get(name).foreach(_ => destroy(name))
    getOrElseCreate(name, objGen)
  }

  def destroy[O <: NamedObject](objOfType: O, name: String)
                      (implicit persister: NamedObjectPersister[O]) {
    namesToObjects get (name) map (f => {
      namesToObjects remove (name)
      f
    }) foreach (f => f onComplete {
      case Success(obj) =>
          persister.unpersist(obj.asInstanceOf[O])
      case Failure(t) =>
    })
  }

  override def forget(name: String) {
    namesToObjects remove (name)
  }

  override def getNames(): Iterable[String] = {
    namesToObjects.keys match {
      case answer: Iterable[String] @unchecked => answer
    }
  }

}
