package spark.jobserver

import akka.actor.Props
import spray.routing.{HttpService, RequestContext}

trait ChunkEncodedStreamingSupport {
  this: HttpService =>

  protected def sendStreamingResponse(ctx: RequestContext,
                                    chunkSize: Int,
                                    byteIterator: Iterator[_]): Unit = {
    actorRefFactory.actorOf {
      Props {
        new ChunkEncodingActor(ctx, chunkSize, byteIterator)
      }
    }
  }
}
