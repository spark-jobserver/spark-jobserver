package spark.jobserver.integrationtests.util

object TestHelper {
  def fileToByteArray(fileName : String) : Array[Byte] = {
    try{
      val stream = getClass().getResourceAsStream(s"/$fileName")
      Iterator continually stream.read takeWhile (-1 !=) map (_.toByte) toArray
    } catch {
      case e: Exception =>
        println(s"Could not open $fileName.")
        e.printStackTrace()
        sys.exit(-1)
    }
  }
}