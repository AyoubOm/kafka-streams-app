package com.ayoubom.kafka.apps

import com.ayoubom.kafka.serdes.JsonSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

import java.util.Properties


object ForeignJoin extends App {

  val props: Properties = new Properties
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "foreign-join-test-7")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

  val streams: KafkaStreams = new KafkaStreams(leftForeignKeyJoinTopology, props)

  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))

  private def leftForeignKeyJoinTopology: Topology = {
    val builder = new StreamsBuilder

    val leftTable = builder
      .table[String, LeftValue]("left-topic", Consumed.`with`(Serdes.String(), new JsonSerde[LeftValue]))

    // leftTable.toStream().print(Printed.toSysOut)

    val rightTable = builder
      .table[String, Integer]("right-topic", Consumed.`with`(Serdes.String(), Serdes.Integer()))


    leftTable
      .leftJoin[Integer, String, Integer](
        rightTable,
        leftValue => leftValue.foreignKey,
        (_: LeftValue, foreignValue: Integer) => foreignValue
      )
      .toStream.to("output-join", Produced.`with`(Serdes.String(), Serdes.Integer()))

    builder.build()
  }

}

case class LeftValue(foreignKey: String, name: String)
