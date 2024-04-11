package com.ayoubom.kafka.apps

import com.ayoubom.kafka.utils.IQService
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import java.time.Duration
import java.util.Properties

/*
  Conclusions:
  - For a stream-stream join, do windows close ?
      - Yes, a record at timestamp t is no more joined when stream time - length_of_window > t. For no grace stream join, the length of the windows = beforeMs + afterMs
      - HOWEVER, we have a stream time per topic. stream time of input1 is advanced by events of topic 1 only. The following case can happen:
          - Say afterMs = 10 and beforeMs = 10, and all following records have the same key
              - input1 <— time: 1, v: 1
              -     input2 <— time: 1, v: 1
              -     input2 <— time: 30, v: 30 —-> here stream time of input2 is advanced to t=30 and first record of input2
                                        is no longer joined
              -     input2 <— time: 8, v: 8 (late record) —-> this record is joined with first record of input1,
                                        because stream time of input1 is still at t=1 .. even if its window should be closed (30 - windowSize > 8)


 */

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
        StreamJoined.`with`(Serdes.String(), Serdes.String(), Serdes.String())
        // streamJoined
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
