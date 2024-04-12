package com.ayoubom.kafka.apps

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, EmitStrategy, Produced, TimeWindows}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import java.io.File
import java.time.Duration
import java.util.Properties
import scala.reflect.io.Directory

object WindowedGroupAtClose extends App {

  prerequisite()

  private val streams: KafkaStreams = new KafkaStreams(topology(), props)

  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))

  // TODO: Why when producing records with different timestamps at the same time, results of closed windows do not appear, until we produce again after some time ?

  private def topology(): Topology = {
    val streams = new StreamsBuilder

    val windowSize = Duration.ofSeconds(10)

    streams
      .stream[String, String]("input", Consumed.`with`(Serdes.String(), Serdes.String()))
      .groupByKey()
      .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(windowSize))
      .emitStrategy(EmitStrategy.onWindowClose())
      .count()
      .toStream
      .selectKey((key, _) => s"${key.key()}-${key.window().start()}")
      .to("output", Produced.`with`(Serdes.String(), Serdes.Long()))

    streams.build()
  }

  private def props: Properties = {
    val props: Properties = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-group-at-close")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-permanent-app")
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
    props
  }

  private def prerequisite() = {
    val directory = new Directory(new File("/tmp/kafka-permanent-app"))
    directory.deleteRecursively()
  }
}
