package spark.jobserver.auth

import akka.actor.ActorSystem
import akka.http.caching.LfuCache
import akka.http.caching.scaladsl.Cache
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.model.{FormData, HttpEntity, HttpMethods, HttpRequest}
import com.auth0.jwk.{JwkException, JwkProvider, UrlJwkProvider}
import com.typesafe.config.Config
import io.jsonwebtoken._
import org.apache.shiro.authc.IncorrectCredentialsException
import spark.jobserver.common.akka.web.JsonUtils

import scala.collection.JavaConverters._
import java.net.URL
import java.security.Key
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class KeycloakAuthenticator(override protected val authConfig: Config)
                           (implicit ec: ExecutionContext, s: ActorSystem)
  extends SJSAuthenticator(authConfig) {

  private val baseUrl = f"${authConfig.getString("keycloak.authServerUrl").stripSuffix("/")}/realms/" +
    f"${authConfig.getString("keycloak.realmName")}/protocol/openid-connect"
  private val clientId = authConfig.getString("keycloak.client")
  private val clientSecret = Try(authConfig.getString("keycloak.clientSecret")).toOption

  protected val keyStore: JwkProvider = new UrlJwkProvider(new URL(f"$baseUrl/certs"))
  protected val jwtParser: JwtParser = Jwts.parser()
    .setSigningKeyResolver(new SigningKeyResolver {
      override def resolveSigningKey(header: JwsHeader[T] forSome {type T <: JwsHeader[T]},
                                     claims: Claims): Key =
        resolveSigningKey(header, "")

      override def resolveSigningKey(header: JwsHeader[T] forSome {type T <: JwsHeader[T]},
                                     plaintext: String): Key = {
        Try(keyStore.get(header.getKeyId).getPublicKey).recoverWith {
          case ex: JwkException =>
            logger.error(s"Failed to fetch JWK: ${ex.getMessage}")
            null
        }.get
      }
    })

  private val cache: Cache[String, AuthInfo] = LfuCache.apply

  override protected def authenticate(credentials: BasicHttpCredentials): Option[AuthInfo] = {
    val BasicHttpCredentials(user, pass) = credentials

    val f = cache.getOrLoad(user, _ => {
      getJwtToken(user, pass)
        .map(parse)
    })
      .map(Some(_))
      .recover {
        case _: IncorrectCredentialsException =>
          logger.info(f"ACCESS DENIED (Incorrect credentials), user [$user]")
          cache.remove(user)
          None
        case ex: JwkException =>
          logger.warn(f"ACCESS DENIED (Invalid token), user [$user], ex [${ex.getMessage}]")
          cache.remove(user)
          None
        case ex: Throwable =>
          logger.info(f"ACCESS DENIED (Unexpected exception), user [$user], ex [${ex.getMessage}]")
          cache.remove(user)
          None
      }
    Await.result(f, authTimeout.seconds)
  }

  protected def getJwtToken(username: String, password: String): Future[Jws[Claims]] = {
    val payload = Map("grant_type" -> "password",
      "username" -> username,
      "password" -> password,
      "client_id" -> clientId
    ) ++ clientSecret.map("client_secret" -> _)

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = f"$baseUrl/token",
      entity = FormData(payload).toEntity)
    Http()
      .singleRequest(request)
      .map(res => JsonUtils.mapFromJson(
        res.entity.asInstanceOf[HttpEntity.Strict].data.utf8String))
      .map(m => {
        m.get("access_token")
          .map(_.asInstanceOf[String])
          .getOrElse(throw new IncorrectCredentialsException)
      })
      .map(jwtParser.parseClaimsJws)
  }

  protected def parse(claims: Jws[Claims]): AuthInfo = {
    val body = claims.getBody
    val name = body.get("preferred_username").asInstanceOf[String]
    val clients = body.get("resource_access").asInstanceOf[java.util.Map[String, java.util.Map[String, Any]]]
    val permissions = Try(clients.get(clientId))
      .toOption
      .flatMap(m => {
        Try(m.get("roles")).toOption.map(_.asInstanceOf[java.util.List[String]].asScala)
      })
      .map(mapRoles)
      .getOrElse(Set.empty)
    logger.debug(f"Authenticated $name with roles $permissions")

    new AuthInfo(User(name), Option(permissions)
      .filter(_.nonEmpty)
      .getOrElse(Set(Permissions.ALLOW_ALL)))
  }

  protected def mapRoles(roles: Seq[String]): Set[Permission] = {
    roles.map(Permissions(_)).filter(_.isDefined).flatten.toSet
  }

}
