package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.scalactic.{Every, Good, Or}
import spark.jobserver.api.{JobEnvironment, ValidationProblem, SparkJob => JobBase}

trait SparkTestJob extends JobBase {
  override type JobData = Config

  def validate(sc: C, runtime: JobEnvironment, config: Config): JobData Or Every[ValidationProblem] =
    Good(config)
}

class MyErrorJob extends SparkTestJob {
  override type JobOutput = Int

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput =
    throw new IllegalArgumentException("Foobar")
}

/** @see [[scala.util.control.NonFatal]] */
class MyFatalErrorJob extends SparkTestJob {
  override type JobOutput = Int

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput =
    throw new OutOfMemoryError("this is a fatal error")
}

class ConfigCheckerJob extends SparkTestJob {

  import scala.collection.JavaConverters._

  override type JobOutput = Seq[String]

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput =
    data.root().keySet().asScala.toSeq
}

// A simple test job that sleeps for a configurable time. Used to test the max-running-jobs feature.
class SleepJob extends SparkTestJob {
  override type JobOutput = Long

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    val sleepTimeMillis: Long = data.getLong("sleep.time.millis")
    Thread.sleep(sleepTimeMillis)
    sleepTimeMillis
  }
}

class CacheSomethingJob extends SparkTestJob {
  override type JobOutput = Int

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    val dd = sc.parallelize(Seq(2, 4, 9, 16, 25, 36, 55, 66))
      .map(_ * 2)
    dd.setName("numbers")
    dd.cache()
    dd.sum.toInt
  }
}

class AccessCacheJob extends SparkTestJob {
  override type JobOutput = Int

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    val rdd = sc.getPersistentRDDs.values.head.asInstanceOf[RDD[Int]]
    rdd.sum.toInt
  }
}

class CacheRddByNameJob extends SparkTestJob {

  import org.apache.spark.storage.StorageLevel

  override type JobOutput = Int

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    import scala.concurrent.duration._
    implicit val timeout: FiniteDuration = 2000 millis

    implicit def rddPersister[T]: NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]

    val rdd = runtime.namedObjects.getOrElseCreate(getClass.getSimpleName, {
      // anonymous generator function
      val rdd = sc.parallelize(1 to 5)
      NamedRDD(rdd, forceComputation = false, storageLevel = StorageLevel.NONE)
    })(timeout, rddPersister[Int])

    // RDD should already be in cache the second time
    val NamedRDD(rdd2, _, _) = runtime.namedObjects.get[NamedRDD[Int]](getClass.getSimpleName).get

    assert(rdd2 == rdd.rdd, "Error: " + rdd2 + " != " + Some(rdd))
    rdd.rdd.map { x => x * x }.collect().sum
  }
}

case class Animal(name: String)

class ZookeeperJob extends SparkTestJob {
  override type JobOutput = Array[Animal]

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    val dd = sc.parallelize(Seq(Animal("dog"), Animal("cat"), Animal("horse")))
    dd.filter(animal => animal.name.startsWith("ho")).collect()
  }
}

object SimpleObjectJob extends SparkTestJob {
  override type JobOutput = Int

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    val rdd = sc.parallelize(1 to 3)
    rdd.collect().sum
  }
}

class jobJarDependenciesJob extends SparkTestJob {
  override type JobOutput = scala.collection.Map[String, Long]

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    val loadedClasses = Seq(
      // EmptyClass is provided as a dependency in emptyJar
      getClass.getClassLoader.loadClass("EmptyClass").getName,
      getClass.getClassLoader.loadClass("EmptyClass").getName,
      getClass.getClassLoader.loadClass("EmptyClass").getName
    )
    val input = sc.parallelize(loadedClasses)
    input.countByValue()
  }
}
