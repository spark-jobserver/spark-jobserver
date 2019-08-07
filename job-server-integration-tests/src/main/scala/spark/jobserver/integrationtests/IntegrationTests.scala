package spark.jobserver.integrationtests

import org.scalatest.ConfigMap

object IntegrationTests extends App {

  // Parse args
  if(args.length < 1){
    printUsage()
    sys.exit(-1)
  }
  val sjs1 = args(0)

  // Run all integration tests
  (new BasicApiTests).execute(configMap = ConfigMap(("address", sjs1)))

  def printUsage(){
    println("Usage: IntegrationTests <jobserver-ip>:<jobserver-port>")
  }

}
