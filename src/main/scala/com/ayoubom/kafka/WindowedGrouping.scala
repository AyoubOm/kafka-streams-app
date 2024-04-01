package com.ayoubom.kafka

import org.apache.kafka.common.serialization.{Serde, Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import java.time.Duration
import java.util.Properties

object WindowedGrouping extends App {

  private val streams: KafkaStreams = new KafkaStreams(topology(), props)

  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))


  private def topology(lateness: Duration = Duration.ZERO): Topology = {
    val streamsBuilder = new StreamsBuilder()

    streamsBuilder
      .stream[String, String]("input", Consumed.`with`(Serdes.String, Serdes.String))
      .groupByKey
      .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), lateness))
      .count()
      .toStream
      .to("output", Produced.`with`(windowSerde, Serdes.Long))

    streamsBuilder.build()
  }

  private def props: Properties = {
    val props: Properties = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-grouping")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props
  }

  private def windowSerde: Serde[Windowed[String]] = {
    val windowedSerializer = new TimeWindowedSerializer[String](new StringSerializer)
    val windowedDeserializer = new TimeWindowedDeserializer[String](new StringDeserializer, 60000)
    Serdes.serdeFrom(windowedSerializer, windowedDeserializer)
  }
}
