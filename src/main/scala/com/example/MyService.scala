package com.example

import scala.concurrent._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.actor._
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import spray.can.Http
import spray.http._
import spray.http.HttpMethods._
import spray.http.MediaTypes._
import spray.util._
import akka.io.Tcp
import java.net.InetSocketAddress
import redis.actors.RedisSubscriberActor
import redis.api.pubsub.{ PMessage, Message }
import redis.RedisClient

class MyServiceActor extends MyService {
}

trait MyService extends Actor with SprayActorLogging {
  def receive = {
    // when a new connection comes in we register ourselves as the connection handler
    case c: Http.Connected =>
      val handler = context actorOf Props(classOf[ConnectionHandler], c.remoteAddress)
      sender ! Http.Register(handler)
  }
}

class SubscribeActor(streamer: ActorRef, channels: Seq[String] = Nil, patterns: Seq[String] = Nil) extends RedisSubscriberActor(channels, patterns) {
  override val address: InetSocketAddress = new InetSocketAddress("localhost", 6379)

  def onMessage(message: Message) {
    println(s" message received: $message")
    streamer ! message.data
  }

  def onPMessage(pmessage: PMessage) {
    println(s"pattern message received: $pmessage")
  }
}

class ConnectionHandler(connected: InetSocketAddress) extends Actor with SprayActorLogging {
  log.debug(s"Connection opened: $connected")

  def receive = {

    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      sender ! index

    case HttpRequest(GET, Uri.Path("/stream"), headers, _, _) =>
      val peer = sender
      val start = headers.find(_.lowercaseName == "last-event-id").map(_.value.toInt).getOrElse(0)
      context actorOf Props(new Streamer(peer, start))

    case _: Http.ConnectionClosed =>
      log.debug("Connection closed: $connected")
      context stop self

    case Timedout(_) =>
      sender ! HttpResponse(status = 500, entity = "The request has timed out...")
  }

  lazy val index = HttpResponse(
    entity = HttpEntity(`text/html`,
      """<html>
      <script>
        var evtSource = new EventSource("/stream");
        evtSource.onmessage = function(e) {
          console.log(e.data);
        }
      </script>
        <body>
        Index
        </body>
      </html>"""))

  class Streamer(client: ActorRef, count: Int) extends Actor with SprayActorLogging {
    val max = count + 500
    log.debug("Starting streaming response ...")

    // we use the successful sending of a chunk as trigger for scheduling the next chunk
    client ! ChunkedResponseStart(HttpResponse(entity = HttpEntity(MediaType.custom("text/event-stream; charset=UTF-8"),
      ": this is a test stream"))).withAck(Ok(count))

    val channels = Seq("time")
    
    val s = self
    context.actorOf(Props(classOf[SubscribeActor], s, channels, Seq.empty).withDispatcher("rediscala.rediscala-client-worker-dispatcher"))

    def receive = {
      case Ok(x) if (x == max) =>
        log.info("Finalizing response stream ...")
        client ! MessageChunk("\nStopped...")
        client ! ChunkedMessageEnd
        context.stop(self)

      case Ok(remaining) =>
        log.info("Sending response chunk ...")
        context.system.scheduler.scheduleOnce(10 seconds span) {
          client ! MessageChunk(s"""

:

              """).withAck(Ok(remaining + 1))
        }

      case message: String => 
        println(s" message received in streamer: $message")
        client ! MessageChunk(s"""

data: $message

              """).withAck(Ok(0))

      case x: Http.ConnectionClosed =>
        log.info("Canceling response stream due to {} ...", x)
        context.stop(self)
        
      case v => println(s"unknown: $v")
    }

    // simple case class whose instances we use as send confirmation message for streaming chunks
    case class Ok(remaining: Int)
  }

}