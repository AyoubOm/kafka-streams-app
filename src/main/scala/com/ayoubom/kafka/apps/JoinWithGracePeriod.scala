package com.ayoubom.kafka.apps

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.internals.RocksDbVersionedKeyValueBytesStoreSupplier
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import java.time.Duration
import java.util.Properties

object JoinWithGracePeriod extends App {

  val props: Properties = new Properties
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-with-grace-period")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

  val streams: KafkaStreams = new KafkaStreams(topology, props)

  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))

  private def topology: Topology = {
    val builder = new StreamsBuilder

    val rightTable = builder.table[String, String](
      "right", Consumed.`with`(Serdes.String(), Serdes.String()),
      Materialized.as[String, String](new RocksDbVersionedKeyValueBytesStoreSupplier("table-store", 1000000)))

    val stream = builder.stream[String, String]("left", Consumed.`with`(Serdes.String(), Serdes.String()))

    val joiner: ValueJoiner[String, String, String] = (leftVal: String, rightVal: String) => leftVal + "|" + rightVal

    stream
      .join[String, String](
        rightTable,
        joiner,
        Joined.`with`(Serdes.String(), Serdes.String(), Serdes.String(), "tok", Duration.ofSeconds(10))
      )
      .peek((key, value) => println(s"$key - $value"))
      .to("output", Produced.`with`(Serdes.String(), Serdes.String()))

    builder.build()
  }

}
