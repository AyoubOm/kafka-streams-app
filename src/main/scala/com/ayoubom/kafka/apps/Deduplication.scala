package com.ayoubom.kafka.apps

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, ProcessorSupplier, Record}
import org.apache.kafka.streams.state.{Stores, WindowStore}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import java.time.{Duration, Instant}
import java.util.Properties


object Deduplication extends App {

  private val streams: KafkaStreams = new KafkaStreams(topology(), props)

  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))


  private def topology(): Topology = {

    val streams = new StreamsBuilder()

    streams.addStateStore(
      Stores.windowStoreBuilder(
        Stores.inMemoryWindowStore(
          "my-window-store",
          Duration.ofSeconds(10),
          Duration.ofSeconds(10),
          true
        ),
        Serdes.String(),
        Serdes.String()
      )
    )

    streams
      .stream("input", Consumed.`with`(Serdes.String(), Serdes.String()))
      .process(
        new ProcessorSupplier[String, String, String, String]() {
          override def get(): Processor[String, String, String, String] = {
            new DedupProcessor()
          }
        }, "my-window-store")
      // .peek((key, value) => println(s"record: $key-$value"))
      .to("output", Produced.`with`(Serdes.String(), Serdes.String()))

    streams.build
  }

  private def props: Properties = {
    val props: Properties = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplication-1")
    props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/app1")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
    props
  }
}

class DedupProcessor extends Processor[String, String, String, String] {

  var store: WindowStore[String, String] = null
  var context: ProcessorContext[String, String] = null

  var streamTime: Instant = Instant.ofEpochMilli(1)

  override def init(context: ProcessorContext[String, String]): Unit = {
    store = context.getStateStore("my-window-store")
    this.context = context
    streamTime = Instant.ofEpochMilli(context.currentStreamTimeMs())
  }

  override def process(record: Record[String, String]): Unit = {
    val iterator = store.fetchAll(record.timestamp() - 10000L, record.timestamp() + 10000L)
    while (iterator.hasNext) {
      val keyValue = iterator.next()
      println(s"found record: ${Instant.ofEpochMilli(keyValue.key.window().start())}-${keyValue.value}")
    }
    println()
    iterator.close()
    println(s"previous streamTime=${Instant.ofEpochMilli(context.currentStreamTimeMs())}")
    store.put(record.key(), record.value(), record.timestamp())
  }
}