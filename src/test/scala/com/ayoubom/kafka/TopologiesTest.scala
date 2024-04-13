package com.ayoubom.kafka

import com.ayoubom.kafka.serdes.{JsonSerde, JsonSerializer}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier
import org.apache.kafka.streams.test.TestRecord
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.time.{Duration, Instant}
import java.util.{Date, Properties}
import scala.reflect.io.Directory

class TopologiesTest extends AnyFunSuite with BeforeAndAfterEach {
  override def afterEach(): Unit = {
    val directory = new Directory(new File("/tmp/kafka-streams/"))
    directory.deleteRecursively()
    super.afterEach()
  }

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

  test("session window") {
    val baseTime = Instant.now().minusSeconds(10)

    val sessionWindowSerializer = new SessionWindowedSerializer[String](new StringSerializer)
    val sessionWindowDeserializer = new SessionWindowedDeserializer[String](new StringDeserializer)

    val windowedSerde = Serdes.serdeFrom(sessionWindowSerializer, sessionWindowDeserializer)


    val testDriver: TopologyTestDriver = new TopologyTestDriver(sessionTopology)
    val inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-topic", windowedSerde.deserializer(), new LongDeserializer)

    inputTopic.pipeInput("foo", 3, baseTime)
    inputTopic.pipeInput("bar", 1, baseTime)
    inputTopic.pipeInput("foo", 2, baseTime.plusSeconds(1))
    inputTopic.pipeInput("bar", 1, baseTime.plusSeconds(1))
    inputTopic.pipeInput("bar", 1, baseTime.plusSeconds(2))
    inputTopic.pipeInput("foo", 3, baseTime.plusSeconds(4))
    inputTopic.pipeInput("bar", 1, baseTime.plusSeconds(4))
    inputTopic.pipeInput("bar", 1, baseTime.plusSeconds(5))

    readOutputTopic(outputTopic)
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


  test("foreign key join: INNER 2nd bug bis") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology(), props)
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

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology(), props)
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

    val testDriver: TopologyTestDriver = new TopologyTestDriver(foreignKeyJoinTopology(), props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic2.pipeInput("fk1", 3)

    inputTopic1.pipeInput("pk1", ProductValue("fk1", "pk1"))
    inputTopic1.pipeInput("pk1", ProductValue("fk2", "pk1"))

    readOutputTopic(outputTopic)
  }

  test("join KTable KTable") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(joinKTableKTableTopology, props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val inputTopic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    inputTopic2.pipeInput("key1", 3)

    inputTopic1.pipeInput("key1", ProductValue("fk1", "pk1"))
    inputTopic1.pipeInput("key1", ProductValue("fk1", "pk1"))

    inputTopic2.pipeInput("key1", null)
    inputTopic2.pipeInput("key2", null)
    //  inputTopic1.pipeInput("key1", ProductValue("fk1", "pk1"))

    readOutputTopic(outputTopic)
  }

  test("self join KTable") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(kTableSelfJoinTopology, props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new StringDeserializer)

    inputTopic1.pipeInput("key1", ProductValue("fk1", "pk1"))

    readOutputTopic(outputTopic)
  }

  test("self join KStream") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(kStreamSelfJoinTopology, props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new StringDeserializer)

    val baseTime = Instant.now().minusSeconds(10)

    inputTopic1.pipeInput("key1", ProductValue("fk1", "pk1"), baseTime)
    inputTopic1.pipeInput("key1", ProductValue("fk1", "pk1"), baseTime.plusSeconds(1))

    readOutputTopic(outputTopic)
  }

  test("join KStream KStream") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(joinKStreamKStreamTopology, props)
    val inputTopic1 = testDriver.createInputTopic("product", new StringSerializer, new JsonSerializer[ProductValue])
    val topic2 = testDriver.createInputTopic("merchant", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-join", new StringDeserializer, new IntegerDeserializer)

    val baseTime = new Date(2024 - 1900, 4, 3, 9, 30, 0).toInstant


    topic2.pipeInput("key1", 4, baseTime)

    inputTopic1.pipeInput("key1", ProductValue("", ""), baseTime)
    inputTopic1.pipeInput(null, null, baseTime)


    readOutputTopic(outputTopic)
  }

  test("aggregate on window close with EmitStrategy") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(aggOnWindowCloseTopology, props)
    val inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer, new LongDeserializer)

    val baseTime = new Date(2024 - 1900, 4, 3, 9, 30, 0).toInstant

    inputTopic.pipeInput("key1", 1, baseTime)
    inputTopic.pipeInput("key1", 2, baseTime.plusSeconds(2))
    inputTopic.pipeInput("key1", 3, baseTime.plusSeconds(4))
    inputTopic.pipeInput("key1", 1, baseTime.plusSeconds(11))
    inputTopic.pipeInput("key1", 2, baseTime.plusSeconds(15))
    inputTopic.pipeInput("key1", 1, baseTime.plusSeconds(100))

    readOutputTopic(outputTopic)
  }

  test("aggregate on window close with Suppressed") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(aggWithSuppressedTopology, props)
    val inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer, new IntegerSerializer)
    val outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer, new LongDeserializer)

    val baseTime = new Date(2024 - 1900, 4, 3, 9, 30, 0).toInstant

    inputTopic.pipeInput("key1", 1, baseTime.minusNanos(1))
    inputTopic.pipeInput("key1", 2, baseTime.plusSeconds(2))
    inputTopic.pipeInput("key1", 3, baseTime.plusSeconds(4))
    inputTopic.pipeInput("key1", 1, baseTime.plusSeconds(10))
    inputTopic.pipeInput("key1", 1, baseTime.plusSeconds(20))

    readOutputTopic(outputTopic)
  }

  test("topology with repartitioning") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())

    val testDriver: TopologyTestDriver = new TopologyTestDriver(topologyWithRepartitioning, props)
    val inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer, new StringSerializer)
    val outputTopic = testDriver.createOutputTopic("output-topic", new StringDeserializer, new LongDeserializer)

    val baseTime = new Date(2024 - 1900, 4, 3, 9, 30, 0).toInstant

    inputTopic.pipeInput("key1", "new-key1", baseTime)
    inputTopic.pipeInput("key2", "new-key2", baseTime.plusSeconds(2))
    inputTopic.pipeInput("key3", "new-key1", baseTime.plusSeconds(2))

    println(topologyWithRepartitioning.describe())
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

  private def joinKStreamKTableTopology: Topology = {
    val builder = new StreamsBuilder

    val productTable = builder
      .stream[String, ProductValue]("product", Consumed.`with`(Serdes.String(), new JsonSerde[ProductValue]))

    productTable
      .join(
        builder.table[String, Integer]("merchant", Consumed.`with`(Serdes.String(), Serdes.Integer())),
        (_: ProductValue, merchantRank: Integer) => merchantRank
      )
      .to("output-join", Produced.`with`(Serdes.String(), Serdes.Integer()))

    builder.build()
  }

  private def joinKTableKTableTopology: Topology = {
    val builder = new StreamsBuilder

    val productTable = builder
      .table[String, ProductValue]("product", Consumed.`with`(Serdes.String(), new JsonSerde[ProductValue]))

    productTable
      .leftJoin(
        builder.table[String, Integer]("merchant", Consumed.`with`(Serdes.String(), Serdes.Integer())),
        (_: ProductValue, merchantRank: Integer) => merchantRank
      )
      .toStream
      .to("output-join", Produced.`with`(Serdes.String(), Serdes.Integer()))

    builder.build()
  }

  private def kTableSelfJoinTopology: Topology = {
    val builder = new StreamsBuilder

    val productTable = builder
      .table[String, ProductValue]("product", Consumed.`with`(Serdes.String(), new JsonSerde[ProductValue]))

    productTable
      .join(
        productTable,
        (first: ProductValue, second: ProductValue) => first.merchant + second.merchant
      )
      .toStream
      .to("output-join", Produced.`with`(Serdes.String(), Serdes.String()))

    builder.build()
  }

  private def kStreamSelfJoinTopology: Topology = {
    val builder = new StreamsBuilder

    val valueJoiner: ValueJoiner[ProductValue, ProductValue, String] =
      (left: ProductValue, right: ProductValue) => left.merchant + right.merchant

    val productStream = builder
      .stream[String, ProductValue]("product", Consumed.`with`(Serdes.String(), new JsonSerde[ProductValue]))

    productStream
      .join(
        productStream,
        valueJoiner,
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(2)),
        StreamJoined.`with`(Serdes.String(), new JsonSerde[ProductValue], new JsonSerde[ProductValue])
      )
      .to("output-join", Produced.`with`(Serdes.String(), Serdes.String()))

    builder.build()
  }

  private def joinKStreamKStreamTopology: Topology = {
    val builder = new StreamsBuilder

    val valueJoiner: ValueJoiner[ProductValue, Integer, Integer] = (_: ProductValue, right: Integer) => right

    val productStream = builder
      .stream[String, ProductValue]("product", Consumed.`with`(Serdes.String(), new JsonSerde[ProductValue]))

    productStream
      .leftJoin[Integer, Integer](
        builder.stream[String, Integer]("merchant", Consumed.`with`(Serdes.String(), Serdes.Integer())),
        valueJoiner,
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
        StreamJoined.`with`(Serdes.String(), new JsonSerde[ProductValue], Serdes.Integer())
      )
      .to("output-join", Produced.`with`(Serdes.String(), Serdes.Integer()))

    builder.build()
  }


  private def aggOnWindowCloseTopology: Topology = {
    val streams = new StreamsBuilder

    val windowSize = Duration.ofSeconds(10)

    streams
      .stream[String, Integer]("input-topic", Consumed.`with`(Serdes.String(), Serdes.Integer()))
      .groupByKey()
      .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(windowSize))
      .emitStrategy(EmitStrategy.onWindowClose())
      .count()
      .toStream
      .selectKey((key, _) => s"${key.window().start()}-${key.key()}")
      .to("output-topic", Produced.`with`(Serdes.String(), Serdes.Long()))

    streams.build()
  }

  private def aggWithSuppressedTopology: Topology = {
    // TODO: can we use window suppress on KTables where the keys are not windows (normal keys) ? -> No
    val streams = new StreamsBuilder

    val windowSize = Duration.ofSeconds(10)

    streams
      .stream[String, Integer]("input-topic", Consumed.`with`(Serdes.String(), Serdes.Integer()))
      .groupByKey()
      .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(windowSize))
      .count()
      .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
      .toStream
      .selectKey((key, _) => s"${key.window().start()}-${key.key()}")
      .to("output-topic", Produced.`with`(Serdes.String(), Serdes.Long()))

    streams.build()
  }

  private def topologyWithRepartitioning: Topology = {
    val streams = new StreamsBuilder

    val windowSize = Duration.ofSeconds(10)

    streams
      .stream[String, String]("input-topic", Consumed.`with`(Serdes.String(), Serdes.String()))
      .selectKey((_, value) => value) // causes repartitioning
      .groupByKey()
      .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(windowSize))
      .emitStrategy(EmitStrategy.onWindowClose())
      .count()
      .toStream
      .selectKey((key, _) => s"${key.window().start()}-${key.key()}")
      .to("output-topic", Produced.`with`(Serdes.String(), Serdes.Long()))

    /* Note: From a compiled topology perspective the three processors

        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(windowSize))
        .emitStrategy(EmitStrategy.onWindowClose())

      represent one AGGREGATE node, which is writing into and reading from the internal windowed store
     */

    /* Study of KAFKA-13842
      - Adding a pre-aggregation step (analogous to combineByKey in Hadoop, beam, etc..) may cause loss of performance in most cases
      instead of increasing it. A state store will be needed to hold the pre-aggregation results. Given X keys and N partitions, each task maintains
      X.N rows in its store. In addition to the I/O cost increase, there is a cost in restoration of tasks as we will now have N additional changelog-topics
      that should be restored at restart of the application, or a rebalance of a task.

      - It may (...or not) increase performance when the windows are large enough which causes big number of records to be shuffled.
     */

    streams.build()
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
