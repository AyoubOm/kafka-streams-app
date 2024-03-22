package com.ayoubom.kafka

import myapps.serdes.{JsonSerde, JsonSerializer}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier
import org.apache.kafka.streams.test.TestRecord
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Duration, Instant}
import java.util.Properties

class TopologiesTest extends AnyFunSuite {

  test("test driver") {
    val testDriver: TopologyTestDriver = new TopologyTestDriver(topology)
    val inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer, new IntegerDeserializer)

    inputTopic.pipeInput("hmida", 1)
    inputTopic.pipeInput("lamba", 2)

    println(outputTopic.readRecord().value())
    println(outputTopic.readRecord().value())
  }


  private def topology: Topology = {
    val builder = new StreamsBuilder
    builder
      .stream[String, Integer]("input-topic", Consumed.`with`(Serdes.String(), Serdes.Integer()))
      .mapValues(value => new Integer(Math.pow(value.intValue(), 2).intValue()))
      .to("output-topic", Produced.`with`(Serdes.String(), Serdes.Integer()))

    builder.build()
  }

  test("window topology") {
    val sd = setUpDriver(windowTopology, "earnings", "agg-earnings")

    val baseTime = Instant.now().minusSeconds(10)

    sd.inputTopic.pipeInput("hmida", 10, baseTime)
    sd.inputTopic.pipeInput("hmida", 20, baseTime)
    sd.inputTopic.pipeInput("hmida", 15, baseTime)
    sd.inputTopic.pipeInput("lambda", 50, baseTime.plusSeconds(1))
    sd.inputTopic.pipeInput("hmida", 30, baseTime.plusSeconds(1))

    val store: WindowStore[String, Integer] = sd.driver.getWindowStore("window_store")

    val iterator = store.fetchAll(baseTime.minusSeconds(1), baseTime.plusSeconds(1))

    println(s"baseTime = $baseTime")
    while (iterator.hasNext) {
      val valueAndTime = iterator.next()
      println(
        s"Window=[${Instant.ofEpochMilli(valueAndTime.key.window().start)}, ${Instant.ofEpochMilli(valueAndTime.key.window().end)}]" +
          s" - key=${valueAndTime.key.key} - value=${valueAndTime.value}")
    }

    readOutputTopic(sd.outputTopic)
  }

  test("foreign key join: 1 st bug") {
    // bug when changing the foreign key to either a null or a non-null value (a wrong event with a null foreign key is output in the join)

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology())
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic1.pipeInput("3 bands", ProductValue("adidas", "3 bands"))
    inputTopic2.pipeInput("adidas", 3)
    inputTopic2.pipeInput("puma", 4)
    inputTopic1.pipeInput(new TestRecord[String, ProductValue]("3 bands", ProductValue(null, "3 bands")))

    readOutputTopic(outputTopic)
  }

  test("foreign key join: 2nd bug") {
    // bug when deleting a left entry (i.e. existing primary key in the join result), we output twice the deletion


    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology())
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic1.pipeInput("3 bands", ProductValue("adidas", "3 bands"))
    inputTopic2.pipeInput("adidas", 3)
    inputTopic2.pipeInput("puma", 4)
    inputTopic1.pipeInput(new TestRecord[String, ProductValue]("3 bands", null))

    readOutputTopic(outputTopic)
  }

  test("foreign key join: inner incoherent behavior") {
    // 4. Incoherence of behavior in INNER Join: changing FK to a non-existent value in right table VS changing FK to a null value
    // In first case we return an event with null right side (inorder to unset the join entry), in the second case we return nothing

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology(true))
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic1.pipeInput("macbook m2", ProductValue("apple", ""))
    inputTopic2.pipeInput("apple", 3)
    inputTopic1.pipeInput("macbook m2", ProductValue("non_existent", ""))
    // vs // inputTopic1.pipeInput("macbook m2", ProductValue(null))

    readOutputTopic(outputTopic)
  }

  test("foreign key join: inner") {

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology(true))
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic1.pipeInput("macbook m2", ProductValue(null, ""))
    inputTopic2.pipeInput("apple", 3)
    inputTopic1.pipeInput("macbook m2", ProductValue("apple", ""))

    readOutputTopic(outputTopic)
  }

  private def windowTopology: Topology = {
    val builder = new StreamsBuilder

    val windowSize = Duration.ofSeconds(1)
    val tumblingWindow = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(windowSize)

    val windowedSerializer = new TimeWindowedSerializer[String](new StringSerializer)
    val windowedDeserializer = new TimeWindowedDeserializer[String](new StringDeserializer, windowSize.toMillis)
    val windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer)

    val storeSupplier =
      new RocksDbWindowBytesStoreSupplier(
        "window_store",
        Duration.ofSeconds(10).toMillis,
        Duration.ofSeconds(10).toMillis,
        windowSize.toMillis,
        false,
        false)

    class IntegerAdd extends Reducer[Integer] {
      override def apply(value1: Integer, value2: Integer): Integer = value1 + value2
    }


    builder
      .stream[String, Integer]("earnings", Consumed.`with`(Serdes.String(), Serdes.Integer()))
      .groupByKey()
      .windowedBy(tumblingWindow)
      .reduce(
        new IntegerAdd,
        Materialized.as(storeSupplier).withKeySerde(Serdes.String()).withValueSerde(Serdes.Integer())
      )
      .toStream
      .to("agg-earnings", Produced.`with`(windowedSerde, Serdes.Integer()))

    builder.build()
  }

  private def foreignKeyJoinTopology(inner: Boolean = false): Topology = {
    val builder = new StreamsBuilder


    val productTable = builder
      .table[String, ProductValue]("product", Consumed.`with`(Serdes.String(), new JsonSerde[ProductValue]))

    if (!inner) {
      productTable
        .leftJoin[Integer, String, Integer](
          builder.table[String, Integer]("merchant", Consumed.`with`(Serdes.String(), Serdes.Integer())),
          product => product.merchant,
          (_: ProductValue, merchantRank: Integer) => merchantRank
        )
    } else {
      productTable
        .join[Integer, String, Integer](
          builder.table[String, Integer]("merchant", Consumed.`with`(Serdes.String(), Serdes.Integer())),
          product => product.merchant,
          (_: ProductValue, merchantRank: Integer) => merchantRank
        )
    }
      .toStream
      .to("output-join", Produced.`with`(Serdes.String(), Serdes.Integer()))

    builder.build()
  }

  private def sessionTopology: Topology = {
    val builder = new StreamsBuilder

    val sessionWindowSerializer = new SessionWindowedSerializer[String](new StringSerializer)
    val sessionWindowDeserializer = new SessionWindowedDeserializer[String](new StringDeserializer)

    val windowedSerde = Serdes.serdeFrom(sessionWindowSerializer, sessionWindowDeserializer)

    builder
      .stream[String, Integer]("input-topic", Consumed.`with`(Serdes.String(), Serdes.Integer()))
      .groupByKey
      .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(2)))
      .count()
      .toStream
      .to("output-topic", Produced.`with`(windowedSerde, Serdes.Long()))

    builder.build()
  }

  private def setUpDriver(topology: Topology, inputTopicName: String, outputTopicName: String): StreamsDriver = {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(topology, props)
    val inputTopic = testDriver.createInputTopic(inputTopicName, new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic(outputTopicName, new StringDeserializer, new IntegerDeserializer)

    StreamsDriver(testDriver, inputTopic, outputTopic)
  }

  private case class StreamsDriver(
                                    driver: TopologyTestDriver,
                                    inputTopic: TestInputTopic[String, Integer],
                                    outputTopic: TestOutputTopic[String, Integer]
                                  )


  private def readOutputTopic(topic: TestOutputTopic[_, _]): Unit = {
    while (!topic.isEmpty) {
      println(topic.readKeyValue())
    }
  }

}

case class ProductValue(merchant: String, name: String)
