package io.mewbase.router

import java.util.concurrent.LinkedBlockingQueue

import akka.http.scaladsl.model.HttpEntity.ChunkStreamPart

import io.mewbase.eventsource.impl.http.SubscriptionRequest
import io.mewbase.eventsource.impl.http.SubscriptionRequest.SubscriptionType
import io.mewbase.eventsource.{Event, EventHandler, EventSource, Subscription}



case class SubscriptionPushPull(eventSource : EventSource, subsRq : SubscriptionRequest ) {

  // not really using as a buffer more as a venue for push pull
  val queue = new LinkedBlockingQueue[ChunkStreamPart](16)

  /* Blocks on the queue if there is no current events */
  def pull() = queue.take()

  import io.mewbase.eventsource.impl.http.HttpEvent
  // define the handler to push to
  val handler =  new EventHandler {
      override def onEvent(evt: Event): Unit = {
        val httpEvent = new HttpEvent(evt)
        queue.offer(httpEvent.toBson().encode.getBytes)
      }
    }

  import SubscriptionType._
  val channel = subsRq.channel
  val subscription =  subsRq.`type` match {
      case  FromNow => eventSource.subscribe(channel, handler)
      case  FromMostRecent => eventSource.subscribeFromMostRecent(channel, handler)
      case  FromEventNumber => eventSource.subscribeFromEventNumber(channel, subsRq.startInclusive, handler)
      case  FromInstant => eventSource.subscribeFromInstant(channel,subsRq.startInstant,handler)
      case  FromStart => eventSource.subscribeAll(channel, handler)
    }

}
