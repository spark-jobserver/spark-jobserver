package spark.jobserver.util

import java.net.{Inet4Address, InetAddress, NetworkInterface}
import org.slf4j.LoggerFactory
import org.apache.commons.lang3.SystemUtils
import scala.collection.JavaConverters._

object NetworkAddressFactory {
  def apply(shortName: String): NetworkAddressStrategy = {
    shortName.toLowerCase() match {
      case "akka" => new Akka
      case "manual" => new Manual
      case "auto" => new Auto
      case unsupported @ _ => throw UnsupportedNetworkAddressStrategy(unsupported)
    }
  }
}

sealed trait NetworkAddressStrategy {
  def getAddress(): Option[String]
}

/**
  * Leaving the akka.remote.netty.tcp.hostname empty means
  * that Akka will automatically find the right address
  * based on InetAddress.getLocalHost.getHostAddress
  */
private class Akka extends NetworkAddressStrategy {
  override def getAddress(): Option[String] = Some("")
}

/**
  * Offload the responsibility of providing the correct IP to user.
  */
private class Manual extends NetworkAddressStrategy {
  override def getAddress(): Option[String] = None
}

/**
  * Jobserver takes the responsibility of finding the right
  * externally accessible IP by reusing SPARK_LOCAL_IP env
  * variable or by finding any IPv4. This function eliminates
  * loopback addresses and IPv6.
  *
  * This strategy should be used when the default hostname
  * does not point to the correct IP and user doesn't have much
  * control over /etc/hosts file.
  *
  * If /etc/hosts file can be manipulated then Akka strategy
  * should be used. Make sure that hostname points to externally
  * accessible IP in /etc/hosts file.
  *
  * Note: Tested on Unix/Linux only
  * Inspired by code from Spark - https://bit.ly/2ORAvz2
  */
private class Auto extends NetworkAddressStrategy {
  private val logger = LoggerFactory.getLogger("AutoIPResolver")

  override def getAddress(): Option[String] = {
    val defaultIpOverride = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverride != null) {
      logger.info(s"Found SPARK_LOCAL_IP env variable with $defaultIpOverride")
      Some(defaultIpOverride)
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
        val reOrderedNetworkIFs =
          if (SystemUtils.IS_OS_WINDOWS) activeNetworkIFs else activeNetworkIFs.reverse

        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.asScala
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if (addresses.nonEmpty) {
            val ip4Addresses = addresses.find(_.isInstanceOf[Inet4Address])
            ip4Addresses match {
              case None =>
              case Some(inetAddress) =>
                // We've found an address that looks reasonable!
                logger.info(s"Hostname, ${InetAddress.getLocalHost.getHostName} resolves to" +
                  s" a loopback address: ${address.getHostAddress}; using " +
                  s"${inetAddress.getHostAddress} instead (on interface ${ni.getName})")
                return Some(inetAddress.getHostAddress)
            }
          }
        }
        logger.error(s"Hostname, ${InetAddress.getLocalHost.getHostName} resolves to" +
          s" a loopback address: ${address.getHostAddress}, but we couldn't find any" +
          " external IP address!")
        throw NoIPAddressFoundException()
      } else {
        logger.info(s"Your hostname resolved to ${address.getHostAddress()}")
        Some(address.getHostAddress())
      }
    }
  }
}