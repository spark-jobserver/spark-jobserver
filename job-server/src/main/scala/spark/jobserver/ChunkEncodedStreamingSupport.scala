//package spark.jobserver
//
//import akka.actor.{ActorSystem, Props}
//import akka.http.scaladsl.server.{RequestContext, Route}
//
//trait ChunkEncodedStreamingSupport {
//  this: Route =>
//
//  protected def sendStreamingResponse(ctx: RequestContext,
//                                    chunkSize: Int,
//                                    byteIterator: Iterator[_])(implicit system: ActorSystem): Unit = {
//    system.actorOf {
//      Props {
//        new ChunkEncodingActor(ctx, chunkSize, byteIterator)
//      }
//    }
//  }
//}
