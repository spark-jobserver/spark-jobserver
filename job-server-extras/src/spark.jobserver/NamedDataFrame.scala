package spark.jobserver

import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.storage.StorageLevel

case class NamedDataFrame(df: DataFrame, forceComputation: Boolean,
                          storageLevel: StorageLevel) extends NamedObject

class DataFramePersister extends NamedObjectPersister[NamedDataFrame] {
  override def saveToContext(namedObj: NamedDataFrame, name: String) {
    namedObj match {
      case NamedDataFrame(df, forceComputation, storageLevel) =>
        require(!forceComputation || storageLevel != StorageLevel.NONE,
          "forceComputation implies storageLevel != NONE")
        //these are not supported by DataFrame:
        //df.setName(name)
        //df.getStorageLevel match
        df.persist(storageLevel)
        // perform some action to force computation
        if (forceComputation) df.count()
    }
  }

  def getFromContext(c: ContextLike, name: String): NamedDataFrame = {
    val df = c.asInstanceOf[SQLContext].table(name)
    //TODO - which StorageLevel ?
    NamedDataFrame(df, false, StorageLevel.NONE)
  }

  /**
   * Calls df.persist(), which updates the DataFrame's cached timestamp, meaning it won't get
   * garbage collected by Spark for some time.
   * @param namedDF the NamedDataFrame to refresh
   */
  override def refresh(namedDF: NamedDataFrame): NamedDataFrame = namedDF match {
    case NamedDataFrame(df, _, storageLevel) =>
      df.persist(storageLevel)
      namedDF
  }

}