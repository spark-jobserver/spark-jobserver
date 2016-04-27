package spark.jobserver

import akka.actor.{Actor, ActorLogging}
import spark.jobserver.ChunkEncodingActor.Ok
import spray.can.Http
import spray.http._
import spray.routing.RequestContext

object ChunkEncodingActor {
  // simple case class whose instances we use as send confirmation message for streaming chunks
  case class Ok(remaining: Iterator[_])
}

/**
  * Performs sending back a response in streaming fashion using chunk encoding
  * @param ctx RequestContext which has the responder to send chunks to
  * @param chunkSize The size of each chunk
  * @param byteIterator Iterator of data to stream back (currently only Byte is supported)
  */
class ChunkEncodingActor(ctx: RequestContext,
                         chunkSize: Int,
                         byteIterator: Iterator[_]) extends Actor with ActorLogging {
  // we use the successful sending of a chunk as trigger for sending the next chunk
  ctx.responder ! ChunkedResponseStart(
    HttpResponse(entity = HttpEntity(MediaTypes.`application/json`,
      byteIterator.take(chunkSize).map {
        case c: Byte => c
      }.toArray))).withAck(Ok(byteIterator))

  def receive: Receive = {
    case Ok(remaining) =>
      val arr = remaining.take(chunkSize).map {
        case c: Byte => c
      }.toArray
      if (arr.nonEmpty) {
        ctx.responder ! MessageChunk(arr).withAck(Ok(remaining))
      }
      else {
        ctx.responder ! ChunkedMessageEnd
        context.stop(self)
      }
    case ev: Http.ConnectionClosed => {
      log.warning("Stopping response streaming due to {}", ev)
      context.stop(self)
    }
  }
}

