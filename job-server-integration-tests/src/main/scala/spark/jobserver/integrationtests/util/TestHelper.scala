package spark.jobserver.integrationtests.util

import java.security.KeyManagementException
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import java.security.cert.X509Certificate

import com.softwaremill.sttp._

import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSession
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import play.api.libs.json.Json

object TestHelper {

  def fileToByteArray(fileName: String): Array[Byte] = {
    try {
      val stream = getClass().getResourceAsStream(s"/$fileName")
      Iterator continually stream.read takeWhile (-1 !=) map (_.toByte) toArray
    } catch {
      case e: Exception =>
        println(s"Could not open $fileName.")
        e.printStackTrace()
        sys.exit(-1)
    }
  }

  def waitForContextTermination(sjs: String, contextName: String, retries: Int) {
    implicit val backend = HttpURLConnectionBackend()
    val SLEEP_BETWEEN_RETRIES = 1000;
    val request = sttp.get(uri"$sjs/contexts/$contextName")
    var response = request.send()
    var json = Json.parse(response.body.merge)
    var state = (json \ "state").as[String]
    var retriesLeft = retries
    while (state != "FINISHED" && retriesLeft > 0) {
      Thread.sleep(SLEEP_BETWEEN_RETRIES)
      response = request.send()
      json = Json.parse(response.body.merge)
      state = (json \ "state").as[String]
      retriesLeft -= 1
    }
  }

  def waitForJobTermination(sjs: String, jobId: String, retries: Int) {
    implicit val backend = HttpURLConnectionBackend()
    val request = sttp.get(uri"$sjs/jobs/$jobId")
    var response = request.send()
    var json = Json.parse(response.body.merge)
    var state = (json \ "status").as[String]
    var retriesLeft = retries
    while (state != "FINISHED" && retriesLeft > 0) {
      Thread.sleep(1000)
      response = request.send()
      json = Json.parse(response.body.merge)
      state = (json \ "status").as[String]
      retriesLeft -= 1
    }
  }

  def disableSSLVerification(): Unit = {
    val trustAllCerts = Array[TrustManager](new X509TrustManager() {
      def getAcceptedIssuers: Array[X509Certificate] = Array[X509Certificate]()
      override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = ()
      override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = ()
    })

    var sc: SSLContext = null
    try {
      sc = SSLContext.getInstance("TLS")
      sc.init(Array[KeyManager](), trustAllCerts, new SecureRandom())
    } catch {
      case e: KeyManagementException =>
        e.printStackTrace()
      case e: NoSuchAlgorithmException =>
        e.printStackTrace()
    }
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory)
    val allHostsValid = new HostnameVerifier {
      override def verify(s: String, sslSession: SSLSession): Boolean = true
    }
    HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid)
  }

}