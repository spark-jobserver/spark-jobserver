package spark.jobserver

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart
import akka.http.scaladsl.model.{ContentType, HttpEntity, MediaTypes}
import akka.http.scaladsl.server.{RequestContext, RouteResult}
import akka.stream.scaladsl.Source

import scala.concurrent.Future

case class ChunkedIterator(it: Iterator[Byte], chunkSize: Int) extends Iterator[ChunkStreamPart] {

  var cache: Option[Array[Byte]] = None

  override def hasNext: Boolean = {
    val nextChunk = it.take(chunkSize).toArray
    cache = if (nextChunk.isEmpty) None else Some(nextChunk)
    cache.isDefined
  }

  override def next(): ChunkStreamPart = ChunkStreamPart(cache.get)
}

trait ChunkEncodedStreamingSupport {

  def actorRefFactory: ActorSystem

  protected def sendStreamingResponse(ctx: RequestContext,
                                      chunkSize: Int,
                                      byteIterator: Iterator[Byte]): Future[RouteResult] = {
    ctx.complete(HttpEntity.Chunked(ContentType(MediaTypes.`application/json`),
      Source.fromIterator(() => ChunkedIterator(byteIterator, chunkSize))))
  }
}
