package spark.jobserver

import spray.http._
import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.routing._

// origin of code:
// first example: https://gist.github.com/joseraya/176821d856b43b1cfe19
// improvement: https://gist.github.com/waymost/4b5598523c2c7361abea
// see also https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS
trait CORSSupport {
  this: HttpService =>

  private val allowOriginHeader = `Access-Control-Allow-Origin`(AllOrigins)
  private val optionsCorsHeaders = List(
    `Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Accept-Encoding, " +
      "Accept-Language, Host, Referer, User-Agent"),
    `Access-Control-Max-Age`(1728000)
  )

  def cors[T]: Directive0 = mapRequestContext { ctx =>
    ctx.withRouteResponseHandling {
      // OPTION request for a resource that responds to other methods
      case Rejected(x) if (ctx.request.method.equals(HttpMethods.OPTIONS)
        && x.exists(_.isInstanceOf[MethodRejection])) => {
        val allowedMethods: List[HttpMethod] =
          x.collect { case rejection: MethodRejection => rejection.supported }
        ctx.complete {
          HttpResponse().withHeaders(
            `Access-Control-Allow-Methods`(HttpMethods.OPTIONS, allowedMethods :_*) ::
              allowOriginHeader :: optionsCorsHeaders
          )
        }
      }
    }.withHttpResponseHeadersMapped { headers =>
      allowOriginHeader :: headers
    }
  }
}