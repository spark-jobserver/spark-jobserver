package spark.jobserver.routes
import com.typesafe.config.Config
import spark.jobserver.auth.AuthInfo

trait ConfigHelper {

    protected def getContextConfig(
      baseConfig: Config,
      fallbackParams: Map[String, String] = Map()
    ): Config

    protected def determineProxyUser(aConfig: Config,
                                     authInfo: AuthInfo,
                                     contextName: String): (String, Config)
}
