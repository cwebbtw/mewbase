package io.mewbase.router


import java.util.UUID

import io.mewbase.bson.{BsonCodec, BsonObject}
import io.mewbase.eventsource.{EventSink, EventSource}
import io.mewbase.eventsource.impl.http.{HttpEventSink, SubscriptionRequest}
import io.netty.buffer.Unpooled
import io.netty.channel._
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.websocketx._
import io.netty.util.CharsetUtil
import io.vertx.core.buffer.Buffer
import org.slf4j.LoggerFactory

import scala.collection.mutable



case class HttpEventRouterHandler(val eventSink : EventSink,
                                  val eventSource : EventSource,
                                  val subscriptionRequests : mutable.Map[UUID, SubscriptionRequest],
                                  val subscriptionHandlers : mutable.Map[Channel, HttpSubscriptionHandler])
                                                    extends SimpleChannelInboundHandler[AnyRef] {

    val logger = LoggerFactory.getLogger(classOf[HttpEventRouterHandler])


    @Override
    @throws[Exception]
    override protected def channelRead0(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
      // basic dispatch on request type
      msg match {
        case httpRq : FullHttpRequest => handleHttpRequest(ctx, httpRq)
        case wsFrame : WebSocketFrame => handleWebSocketFrame(ctx, wsFrame)
      }
    }


  @throws[Exception]
  def handleHttpRequest(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {

    req.method match {

      case HttpMethod.GET => {
        req.uri match {
          case "/ping" => complete(ctx,"pong")

          case _ => {
            // this get could be an upgrade to web socket request.
            val upgradeHeader = req.headers.get("Upgrade")
            if (upgradeHeader != null && "websocket".equalsIgnoreCase(upgradeHeader)) {
              val url = "ws://" + req.headers.get("Host") + req.uri
              val wsFactory = new WebSocketServerHandshakerFactory(url, null, false)
              val handshaker = wsFactory.newHandshaker(req)
              if (handshaker == null) {
                logger.debug("Handshaker is null")
                WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel)
              }  else {
                logger.debug("Upgrading Handlers to Web Sockets Frame Handlers")
                handshaker.handshake(ctx.channel, req)

                // Socket is upgraded so now we can start the subscription
                val uuid = UUID.fromString(req.uri.replace("/",""))
                subscriptionRequests.remove(uuid) match {
                  case Some(request) => {
                    val handler = HttpSubscriptionHandler(ctx.channel,handshaker,eventSource,request)
                    subscriptionHandlers.put(ctx.channel, handler)
                  }
                  case None => {
                    logger.error("Failed to find subscription request for uri "+req.uri)
                    handshaker.close(ctx.channel,new CloseWebSocketFrame())
                  }
                }
              }

            } else {
              // or its just a random Get request that we dont handle
              error(ctx, HttpResponseStatus.BAD_REQUEST)
            }
          }
        }
      }


      // If you're going to do normal HTTP POST authentication before upgrading the
      // WebSocket, the recommendation is to handle it  here
      case HttpMethod.POST => {
        req.uri match {
          case "/publish" => {
            // use Vert.x buffer to copy out PooledUnsafeDirectByteBuf contents
            val publishBody = BsonCodec.bsonBytesToBsonObject(req.content().array())
            val channel = publishBody.getString(HttpEventSink.CHANNEL_TAG)
            val event = publishBody.getBsonObject(HttpEventSink.EVENT_TAG)
            logger.debug("Published on channel "+channel+" event "+ event)
            val eventNumber = eventSink.publishSync(channel,event)
            complete(ctx,eventNumber.toString)
          }

          case "/subscribe" => {
            // use Vert.x buffer to copy out PooledUnsafeDirectByteBuf contents
            val binaryRequest = BsonCodec.bsonBytesToBsonObject(req.content().array())
            val subscriptionRequest = new SubscriptionRequest(binaryRequest)
            val subscriptionUUID = UUID.randomUUID()
            subscriptionRequests.put(subscriptionUUID,subscriptionRequest)
            logger.debug("New subscription request with UUID:"+subscriptionUUID+": "+subscriptionRequest)
            complete(ctx,subscriptionUUID.toString())
          }

          case _ => { error(ctx, HttpResponseStatus.BAD_REQUEST) }
        }
      }

      case _  => error(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED)
    }

  }


  def handleWebSocketFrame(ctx: ChannelHandlerContext, frame: WebSocketFrame): Unit = {
    logger.debug("Received incoming ws frame [{}]", frame.getClass.getName)

    frame match {
      case _ : CloseWebSocketFrame => {
        logger.debug("Close frame received on WebSocket")
        subscriptionHandlers.remove(ctx.channel) match {
          case Some(handler) => handler.stop
          case None => logger.error("Failed to associate closing channel with corresponding handler")
        }
      }
      case  _ => logger.error("Received non closing web socket frame from client")
    }
  }


  // Complete a successful response with a given body
  def complete(ctx: ChannelHandlerContext, msg: String) = {
    val status = HttpResponseStatus.OK
    val message = Unpooled.copiedBuffer(msg, CharsetUtil.UTF_8)
    val response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, message)
    response.headers.set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8")
    // Close the connection as soon as the content is sent.
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
  }


  // Send an unsuccessful response with a given status response
  def error(ctx: ChannelHandlerContext, status : HttpResponseStatus ) = {
      val message = Unpooled.copiedBuffer("Failure: " + status , CharsetUtil.UTF_8)
      val response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, message)
      response.headers.set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8")
      // Close the connection as soon as the error message is sent.
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
  }



}
