package com.ayoubom.kafka

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.scalatest.funsuite.AnyFunSuite

class InnerFKJoinTest extends AnyFunSuite {

  test("foreign key join inner") {

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology)
    val leftTopic = testDriver.createInputTopic("left-topic", new StringSerializer, new JsonSerializer[LeftRecord])
    val rightTopic = testDriver.createInputTopic("right-topic", new StringSerializer, new StringSerializer)
    val outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer, new StringDeserializer)

    rightTopic.pipeInput("fk", "1")

    leftTopic.pipeInput("pk1", LeftRecord(null, "pk1"))
    leftTopic.pipeInput("pk1", LeftRecord("fk", "pk1"))

    readOutputTopic(outputTopic)
  }

  private def foreignKeyJoinTopology: Topology = {
    val builder = new StreamsBuilder

    val leftTable = builder
      .table[String, LeftRecord]("left-topic", Consumed.`with`(Serdes.String(), new JsonSerde[LeftRecord]))

    val rightTable = builder.table[String, String]("right-topic", Consumed.`with`(Serdes.String(), Serdes.String()))

    leftTable
      .join[String, String, String](
        rightTable,
        leftRecord => leftRecord.foreignKey,
        (_: LeftRecord, rightValue: String) => rightValue
      )
      .toStream
      .to("output-topic", Produced.`with`(Serdes.String(), Serdes.String()))

    builder.build()
  }

  private def readOutputTopic(topic: TestOutputTopic[String, String]): Unit = {
    while (!topic.isEmpty) {
      println(topic.readKeyValue())
    }
  }

}

case class LeftRecord(foreignKey: String, name: String)

