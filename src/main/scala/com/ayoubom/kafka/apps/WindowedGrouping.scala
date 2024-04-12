package com.ayoubom.kafka.apps

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
    // In case of tumbling windows without grace, Can kafka update a previous window even when a new new window starts ??!
    //  How can it be the case since the first window is already closed ?
    //   -> [TESTED] No, once a second window starts, the first window is no longer updated and records happening at that window are
    //    dropped

    /*
    Behaviors
      - No Grace: Kafka Streams uses stream time which is only advanced by events. With NoGrace the window is closed when
       streamTime is beyond its end.

      - Grace: By definition, the window will not be closed when stream time is beyond its end, but rather when stream time
      is beyond its end + grace period

      Q) What is the difference between TimeWindows (tumbling) of length L and grace period G with a sliding window of length L
      which advances by L - G
          -> Answer: An event arriving between L and L + G belongs to the second window in TimeWindows, it on the other side belongs
          to both the first and the second window in the sliding window
     */

    val streamsBuilder = new StreamsBuilder()

    streamsBuilder
      .stream[String, String]("input", Consumed.`with`(Serdes.String, Serdes.String))
      .groupByKey
      .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), lateness))
      .count()
      .toStream
      .selectKey((key, _) => s"${key.key()}-${key.window().startTime()}")
      .to("output", Produced.`with`(Serdes.String(), Serdes.Long))

    streamsBuilder.build()
  }

  private def props: Properties = {
    val props: Properties = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-grouping")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
    props
  }

  private def windowSerde: Serde[Windowed[String]] = {
    val windowedSerializer = new TimeWindowedSerializer[String](new StringSerializer)
    val windowedDeserializer = new TimeWindowedDeserializer[String](new StringDeserializer, 60000)
    Serdes.serdeFrom(windowedSerializer, windowedDeserializer)
  }
}
