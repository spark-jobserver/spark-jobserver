package spark.jobserver.util

import java.io.FileInputStream
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManager, TrustManagerFactory}

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

/**
  * Creates SSLContext based on configuration, the SSLContext is used for communication between
  * the SJS web server and its clients.
  *
  * If encryption is not activated in the configuration, then the default ssl context is used which
  * falls back to un-encrypted communication.
  */
object SSLContextFactory {

  val logger = LoggerFactory.getLogger(getClass)

  def createContext(config: Config): SSLContext = {

    if (config.hasPath("ssl-encryption") && config.getBoolean("ssl-encryption")) {

      checkRequiredParamsSet(config)
      val encryptionType = config.getString("encryptionType")
      val sslContext = SSLContext.getInstance(encryptionType)
      logger.info(encryptionType + " encryption activated.")

      val keyStoreFileName = config.getString("keystore")
      val keyStorePassword = config.getString("keystorePW").toCharArray()
      val keyStoreType = config.getString("keystoreType")
      val keyStore = KeyStore.getInstance(keyStoreType)
      keyStore.load(new FileInputStream(keyStoreFileName), keyStorePassword)
      val keyManagerFactory = KeyManagerFactory.getInstance(config.getString("provider"))
      keyManagerFactory.init(keyStore, keyStorePassword)
      logger.info("Using KeyStore: " + keyStoreFileName)

      var trustManagers: Array[TrustManager] = null
      if (config.hasPath("truststore")) {
        val trustStoreFileName = config.getString("truststore")
        val trustStorePassword = config.getString("truststorePW").toCharArray()
        val trustStoreType = config.getString("truststore-type")
        val trustStore = KeyStore.getInstance(trustStoreType)
        trustStore.load(new FileInputStream(trustStoreFileName), trustStorePassword)
        val trustManagerFactory = TrustManagerFactory.getInstance(config.getString("truststore-provider"))
        trustManagerFactory.init(trustStore)
        trustManagers = trustManagerFactory.getTrustManagers
        logger.info("Using TrustStore: " + trustStoreFileName)
      }

      sslContext.init(keyManagerFactory.getKeyManagers(), trustManagers, null)

      sslContext
    } else {
      SSLContext.getDefault
    }
  }

  val MISSING_KEYSTORE_MSG = "Configuration error (param 'keystore'): ssl/tsl encryption is " +
    "activated, but keystore location is not configured."
  val MISSING_KEYSTORE_PASSWORD_MSG = "Configuration error (param 'keystorePW'): ssl/tsl encryption is " +
    "activated, but keystore password is not configured."

  def checkRequiredParamsSet(config: Config) {
    if (!config.hasPath("keystore")) {
      throw new RuntimeException(MISSING_KEYSTORE_MSG)
    }
    if (!config.hasPath("keystorePW")) {
      throw new RuntimeException(MISSING_KEYSTORE_PASSWORD_MSG)
    }
  }
}
