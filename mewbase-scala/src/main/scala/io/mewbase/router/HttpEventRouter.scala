package io.mewbase.router





import com.typesafe.config.ConfigFactory

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.{HttpObjectAggregator, HttpRequestDecoder, HttpResponseEncoder}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.slf4j.LoggerFactory



object HttpEventRouter extends App {

  // key to find port number
  val PORT_CONFIG_PATH = "mewbase.http.router.port"

  // Configure the HttpRequestDecoder to handle 'small' line lengths
  // and headers and "large" payload chunks for large event encodings
  val initialLineLength = 4096
  val maxHeaderSize = 4096
  val maxPayloadFrameSize = 1024 * 1024


  class LocalChannelInitializer extends ChannelInitializer[SocketChannel] {
    override def initChannel(ch: SocketChannel): Unit = {
      val p = ch.pipeline()
      val decoder = new HttpRequestDecoder(initialLineLength, maxHeaderSize, maxPayloadFrameSize, false)
      p.addLast("decoder", decoder)
      p.addLast("aggregator", new HttpObjectAggregator(65536))
      p.addLast("encoder", new HttpResponseEncoder())
      p.addLast("handler", new HttpEventRouterHandler())
    }
  }


  private val logger = LoggerFactory.getLogger(getClass().getName())

  private val PORT = ConfigFactory.load.getInt(PORT_CONFIG_PATH)

  val bossGroup : NioEventLoopGroup = new NioEventLoopGroup(1)
  val workerGroup : NioEventLoopGroup = new NioEventLoopGroup()

  try {
    val b = new ServerBootstrap()

    b.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .handler(new LoggingHandler(LogLevel.INFO))
      .childHandler(new LocalChannelInitializer() )

    // Start the server.
    val f = b.bind(PORT).sync
    logger.info("Http Event Router started")
    // Wait until the server socket is closed.
    f.channel.closeFuture.sync
    logger.info("Http Event Router closed")
  } finally {
    logger.info("Http Event Router shutdown started")
    // Shut down all event loops to terminate all threads.
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
    logger.info("Http Event Router shutdown completed")
  }


}




