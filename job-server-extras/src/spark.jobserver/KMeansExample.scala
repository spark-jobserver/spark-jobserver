package spark.jobserver

import com.typesafe.config.Config
import org.apache.spark._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

/**
 * A Spark job example that implements the SparkJob trait and can be submitted to the job server.
 *
 * Set the config with the sentence to split or count:
 * input.string = "adsfasdf asdkf  safksf a sdfa"
 *
 * validate() returns SparkJobInvalid if there is no input.string
 */
object KMeansExample extends SparkJob with NamedRddSupport {
  val NUM_ITERATIONS = 100
  val K = 7

  /**
   * Assume that the job succeeds
   * @param sc
   * @param config
   * @return Always return SparkJobValid as this example will not do error checking
   */
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = SparkJobValid

  override def runJob(sc: SparkContext, config: Config): (Array[String], Array[String], Long) = {
    //load the hadoop configuration file, since the job server doesn't seem to do it
    val sqlContext = new SQLContext(sc)

    //try to load namedRDD
    val cacheReturnOption = namedRdds.get[Row]("kmeans")
    if (cacheReturnOption.isDefined) {
      //we can't use the toDF function since a row isn't of the Product interface, so the conversion fails
      val cacheReturn = cacheReturnOption.get.cache()
      val cacheReturnDF = sqlContext.createDataFrame(cacheReturn, cacheReturn.take(1)(0).schema)
      sampleAndReturn(cacheReturnDF)
    }
    else {
      //limit forces
      val data = sqlContext.read.parquet("s3n://us-east-1.elasticmapreduce.samples/flightdata/input")
        .limit(5E6.toInt)
      val intCols = data.dtypes.filter(_._2 == "IntegerType").map(_._1).map(data.col(_))
      val intDF = data.select(intCols: _*).repartition(50)
      //just choose some reasonable na value
      val noNADF = intDF.na.fill(0)

      //since kmeans needs each row to have a feature vector, must assemble all columns into a vector
      val assembler = new VectorAssembler().setInputCols(intCols.map(_.toString())).setOutputCol("Features")
      val featuresDF = assembler.transform(noNADF).cache()

      val stdScalerObject = new StandardScaler()
      stdScalerObject.setWithMean(false)
      stdScalerObject.setWithStd(true)
      stdScalerObject.setInputCol("Features")
      stdScalerObject.setOutputCol("ScaledFeatures")

      val scaler = stdScalerObject.fit(featuresDF)

      val scaledData = scaler.transform(featuresDF).cache()
      scaledData.foreach(_ => ())
      featuresDF.unpersist()

      val kmeans = new KMeans()
      kmeans.setK(K)
      kmeans.setMaxIter(NUM_ITERATIONS)
      kmeans.setFeaturesCol("ScaledFeatures")
      kmeans.setPredictionCol("Output")
      val dataWithPredictions = kmeans.fit(scaledData).transform(scaledData).cache()
      namedRdds.update("kmeans", dataWithPredictions.rdd)
      sampleAndReturn(dataWithPredictions)
    }


  }

  def sampleAndReturn(dataWithPredictions: DataFrame): (Array[String], Array[String], Long) = {
    //take 1000 points
    val sample = dataWithPredictions.drop("Features").drop("ScaledFeatures")
      .sample(false, 1000D / dataWithPredictions.count())
    (sample.columns, sample.toJSON.collect(), sample.count())
  }
}
