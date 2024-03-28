package com.ayoubom.kafka

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.test.TestRecord
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util.Properties
import scala.reflect.io.Directory

class TopologiesTest extends AnyFunSuite with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    val directory = new Directory(new File("/tmp/kafka-streams/"))
    directory.deleteRecursively()
    super.afterEach()
  }

  test("test driver") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(topology, props)
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
  /*
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


   */

  test("foreign key join: INNER 1st bug") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology, props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic2.pipeInput("adidas", 3)

    inputTopic1.pipeInput("pk1", ProductValue(null, "pk1"))
    inputTopic1.pipeInput("pk1", ProductValue("adidas", "pk1"))


    readOutputTopic(outputTopic)
  }

  test("foreign key join: INNER 2nd bug") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology, props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic2.pipeInput("adidas", 3)

    inputTopic1.pipeInput("pk1", ProductValue("adidas", "pk1"))
    inputTopic1.pipeInput("pk1", ProductValue(null, "pk1"))
    inputTopic1.pipeInput("pk1", ProductValue("adidas", "pk1"))
    //inputTopic1.pipeInput("pk1", ProductValue("adidas", "pk1"))


    readOutputTopic(outputTopic)
  }

  test("foreign key join: INNER 2nd bug bis") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology, props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic2.pipeInput("fk1", 3)

    inputTopic1.pipeInput("pk1", ProductValue("fk1", "pk1"))
    inputTopic1.pipeInput("pk1", ProductValue(null, "pk1"))

    readOutputTopic(outputTopic)
  }

  test("foreign key join: INNER 3rd bug") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology, props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)


    inputTopic1.pipeInput("pk1", ProductValue("fk1", "pk1"))
    inputTopic1.pipeInput("pk1", ProductValue("fk1", "pk1")) // unexpected record with null value (I believe?)


    readOutputTopic(outputTopic)
  }

  test("foreign key join: INNER 4th bug ?") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology, props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic2.pipeInput("fk1", 3)

    inputTopic1.pipeInput("pk1", ProductValue("fk1", "pk1"))
    inputTopic1.pipeInput("pk1", ProductValue("fk2", "pk1"))

    readOutputTopic(outputTopic)
  }

  /*
    private def windowTopology: Topology = {
      val builder = new StreamsBuilder

      val windowSize = Duration.ofSeconds(1)
      //val tumblingWindow = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(windowSize)


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
  */
  private def foreignKeyJoinTopology: Topology = {
    val builder = new StreamsBuilder

    builder
      .table[String, ProductValue]("product", Consumed.`with`(Serdes.String(), new JsonSerde[ProductValue]))
      .join[Integer, String, Integer](
        builder.table[String, Integer]("merchant", Consumed.`with`(Serdes.String(), Serdes.Integer())),
        product => product.merchant,
        (_: ProductValue, merchantRank: Integer) => merchantRank
      )
      .toStream
      .to("output-join", Produced.`with`(Serdes.String(), Serdes.Integer()))

    builder.build()
  }

  private def setUpDriver(topology: Topology, inputTopicName: String, outputTopicName: String) = {
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


  private def readOutputTopic(topic: TestOutputTopic[String, Integer]): Unit = {
    while (!topic.isEmpty) {
      println(topic.readKeyValue())
    }
  }

}

case class ProductValue(merchant: String, name: String)
