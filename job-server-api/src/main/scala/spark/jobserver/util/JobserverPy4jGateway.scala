package spark.jobserver.util

import org.apache.commons.lang.RandomStringUtils
import py4j.GatewayServer
import py4j.GatewayServer.GatewayServerBuilder

class JobserverPy4jGateway() {
  private var server: GatewayServer = _
  private val py4jToken = RandomStringUtils.randomAlphanumeric(50)

  def getGatewayPort(endpoint: Any): String = {
    server = new GatewayServerBuilder()
      .entryPoint(endpoint)
      .javaPort(0)
      .authToken(py4jToken)
      .build()

    //Server runs asynchronously on a dedicated thread. See Py4J source for more detail
    server.start()
    server.getListeningPort.toString
  }

  def getToken(): String = {
    this.py4jToken
  }

  def getGateway(): GatewayServer = {
    this.server
  }

  def stop(): Unit = {
    server.shutdown()
  }
}
