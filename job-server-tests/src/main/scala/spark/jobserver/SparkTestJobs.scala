package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.rdd.RDD


trait SparkTestJob extends SparkJob {
  def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid
}

class MyErrorJob extends SparkTestJob {
  def runJob(sc: SparkContext, config: Config): Any = {
    throw new IllegalArgumentException("Foobar")
  }
}

class ConfigCheckerJob extends SparkTestJob {
  import scala.collection.JavaConverters._

  def runJob(sc: SparkContext, config: Config): Any = {
    config.root().keySet().asScala.toSeq
  }
}

// A simple test job that sleeps for a configurable time. Used to test the max-running-jobs feature.
class SleepJob extends SparkTestJob {
  def runJob(sc: SparkContext, config: Config): Any = {
    val sleepTimeMillis: Long = config.getLong("sleep.time.millis")
    Thread.sleep(sleepTimeMillis)
    sleepTimeMillis
  }
}

class CacheSomethingJob extends SparkTestJob {
  def runJob(sc: SparkContext, config: Config): Any = {
    val dd = sc.parallelize(Seq(2, 4, 9, 16, 25, 36, 55, 66))
               .map(_ * 2)
    dd.setName("numbers")
    dd.cache()
    dd.sum.toInt
  }
}

class AccessCacheJob extends SparkTestJob {
  def runJob(sc: SparkContext, config: Config): Any = {
    val rdd = sc.getPersistentRDDs.values.head.asInstanceOf[RDD[Int]]
    rdd.sum.toInt
  }
}

class CacheRddByNameJob extends SparkTestJob with NamedRddSupport {
  def runJob(sc: SparkContext, config: Config): Any = {
    import scala.concurrent.duration._
    implicit val timeout = akka.util.Timeout(100 millis)

    val rdd = namedRdds.getOrElseCreate(getClass.getSimpleName, {
      // anonymous generator function
      sc.parallelize(1 to 5)
    })

    // RDD should already be in cache the second time
    val rdd2 = namedRdds.get[Int](getClass.getSimpleName)
    assert(rdd2 == Some(rdd), "Error: " + rdd2 + " != " + Some(rdd))
    rdd.map { x => x * x }.collect().sum
  }
}

case class Animal(name: String)

class ZookeeperJob extends SparkTestJob {
  def runJob(sc: SparkContext, config: Config): Any = {
    val dd = sc.parallelize(Seq(Animal("dog"), Animal("cat"), Animal("horse")))
    dd.filter(animal => animal.name.startsWith("ho")).collect()
  }
}

object SimpleObjectJob extends SparkTestJob {
  def runJob(sc: SparkContext, config: Config): Any = {
    val rdd = sc.parallelize(1 to 3)
    rdd.collect().sum

  }
}

class jobJarDependenciesJob extends SparkTestJob {
  def runJob(sc: SparkContext, config: Config): Any = {
    val loadedClasses = Seq(
      getClass.getClassLoader.loadClass("spark.jobserver.context.SQLContextFactory").getName(),
      getClass.getClassLoader.loadClass("spark.jobserver.context.HiveContextFactory").getName(),
      getClass.getClassLoader.loadClass("spark.jobserver.context.StreamingContextFactory").getName()
      )
    val input = sc.parallelize(loadedClasses)
    input.countByValue()
  }
}
