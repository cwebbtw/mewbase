package io.mewbase.router



import java.nio.ByteBuffer

import io.mewbase.bson.BsonCodec
import io.mewbase.eventsource.{Event, EventHandler, EventSource, Subscription}
import io.mewbase.eventsource.impl.http.{HttpEvent, SubscriptionRequest}
import io.mewbase.eventsource.impl.http.SubscriptionRequest.SubscriptionType
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.Channel
import io.netty.handler.codec.http.websocketx.{BinaryWebSocketFrame, CloseWebSocketFrame, WebSocketServerHandshaker}
import org.slf4j.LoggerFactory



case class HttpSubscriptionHandler (val nettyChannel : Channel,
                                    val handshaker : WebSocketServerHandshaker,
                                    val eventSource : EventSource,
                                    val subscriptionRequest : SubscriptionRequest) {

  private val logger = LoggerFactory.getLogger(classOf[HttpSubscriptionHandler])

  private val subscriptionTimeOut = 3;

  val handler : EventHandler = (evt: Event) => {
    val bsonEvent = new HttpEvent(evt).toBson()
    if (nettyChannel.isActive) nettyChannel.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(BsonCodec.bsonObjectToBsonBytes(bsonEvent)))) else {
      logger.info("Attempt to write event on an inactive socket - stopping subscription")
      stop
    }
  }

  import SubscriptionType._
  val evtChannel = subscriptionRequest.channel
  val subscriptionFut =  subscriptionRequest.`type` match {
    case  FromNow => eventSource.subscribe(evtChannel, handler)
    case  FromMostRecent => eventSource.subscribeFromMostRecent(evtChannel, handler)
    case  FromEventNumber => eventSource.subscribeFromEventNumber(evtChannel, subscriptionRequest.startInclusive, handler)
    case  FromInstant => eventSource.subscribeFromInstant(evtChannel,subscriptionRequest.startInstant,handler)
    case  FromStart => eventSource.subscribeAll(evtChannel, handler)
  }


  def stop : Unit = {
    subscriptionFut.thenAccept{ (subs ) => subs.close() }
    handshaker.close(nettyChannel, new CloseWebSocketFrame())
  }

}
