package com.ayoubom.kafka

import com.ayoubom.kafka.serdes.streamJoined
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import java.time.Duration
import java.util.Properties

object WindowedJoin extends App {

  private val streams: KafkaStreams = new KafkaStreams(topology, props)

  streams.start()

  IQService.startRestProxy(streams)

  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))


  private def topology: Topology = {
    val streamsBuilder = new StreamsBuilder()

    val valueJoiner: ValueJoiner[String, String, String] = (left: String, right: String) => left + right

    val leftStream = streamsBuilder
      .stream[String, String]("input1", Consumed.`with`(Serdes.String(), Serdes.String()))

    leftStream
      .join[String, String](
        streamsBuilder.stream[String, String]("input2", Consumed.`with`(Serdes.String(), Serdes.String())),
        valueJoiner,
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
        streamJoined
      )
      .to("join", Produced.`with`(Serdes.String(), Serdes.String()))

    streamsBuilder.build()
  }

  private def props: Properties = {
    val props: Properties = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-join")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-permanent-app")
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
    props
  }


}
