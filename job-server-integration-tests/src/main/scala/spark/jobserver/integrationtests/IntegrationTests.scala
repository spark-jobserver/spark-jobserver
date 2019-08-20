package spark.jobserver.integrationtests

import org.scalatest.ConfigMap

object IntegrationTests extends App {

  // Parse args
  if(args.length != 2){
    printUsage()
    sys.exit(-1)
  }
  val sjs1 = args(0)
  val sjs2 = args(1)

  // Run all integration tests
  (new BasicApiTests).execute(configMap = ConfigMap(("address", sjs1)))
  (new CornerCasesTests).execute(configMap = ConfigMap(("address", sjs1)))
  (new HATests).execute(configMap = ConfigMap(("sjs1", sjs1), ("sjs2", sjs2)))

  // Usage
  def printUsage(){
    println("Usage: IntegrationTests <jobserver1-ip>:<jobserver1-port> <jobserver2-ip>:<jobserver2-port>")
  }

}
