package com.ayoubom.kafka

import com.ayoubom.kafka.serdes.{JsonSerde, JsonSerializer}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, ProcessorSupplier, Record}
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier
import org.apache.kafka.streams.state.{Stores, TimestampedKeyValueStore, ValueAndTimestamp, WindowStore}
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

    println(joinKTableKTableTopology.describe())

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

    println(aggOnWindowCloseTopology.describe())

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
      .mapValues((k, v) => v)
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

  test("global store with processor") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(topologyWithGlobalStore, props)
    val globalTopic = testDriver.createInputTopic("global-input", new StringSerializer, new StringSerializer)
    val inputTopic = testDriver.createInputTopic("input", new StringSerializer, new StringSerializer)
    val outputTopic = testDriver.createOutputTopic("output", new StringDeserializer, new StringDeserializer)

    globalTopic.pipeInput("key1", "global-value")
    inputTopic.pipeInput("key1", "value1")

    readOutputTopic(outputTopic)
  }

  private def topologyWithGlobalStore: Topology = {
    val streams = new StreamsBuilder

    val storeBuilder = Stores.timestampedKeyValueStoreBuilder(
      Stores.persistentTimestampedKeyValueStore("my-global-store"),
      Serdes.String(),
      Serdes.String()
    )

    class MyProcessor extends Processor[String, String, Void, Void] {

      var store: TimestampedKeyValueStore[String, String] = null

      override def init(context: ProcessorContext[Void, Void]): Unit = {
        store = context.getStateStore("my-global-store")
      }

      override def process(record: Record[String, String]): Unit = {
        store.put(record.key(), ValueAndTimestamp.make(s"new-${record.value()}", Instant.now().toEpochMilli))
      }
    }

    class MySupplier extends ProcessorSupplier[String, String, Void, Void]() {
      override def get(): Processor[String, String, Void, Void] = {
        new MyProcessor()
      }
    }

    class MyInputProcessor extends Processor[String, String, String, String] {

      var store: TimestampedKeyValueStore[String, String] = null
      var context: ProcessorContext[String, String] = null

      override def init(context: ProcessorContext[String, String]): Unit = {
        store = context.getStateStore("my-global-store")
        this.context = context
      }

      override def process(record: Record[String, String]): Unit = {
        val value = store.get(record.key())
        context.forward(new Record[String, String](record.key, record.value() + "|" + value.value(), Instant.now().toEpochMilli))
      }
    }

    streams.addGlobalStore(
      storeBuilder,
      "global-input",
      Consumed.`with`(Serdes.String(), Serdes.String()),
      new MySupplier()
    )

    // ============== To search 1
    // Q: is it possible to join two KTables or KStream-KTable that don't have the same number of partitions ?
    //  If not possible, why not allowing it, and making a repartition step (as we do with group bys that change the key)
    //  the repartition step would create an internal topic which will repartition the right topic into a topic with same
    //  number of partitions as left topic, and do the join between left topic and this repartitioned topic
    // --> Check Answer to question Q2 in this section

    // One problem: we would need an additional internal store for the repartitioned topic. However, it should exist for KStream-KStream join
    //  (should check if it is possible today) --> According to docs, not possible even for KStream-KStream
    // One problem for KStream-KStream join is that we should repartition the right KStream with the same strategy as the left KStream,
    // but the partitioner strategy may consider also the record's value
    // We could add it as a limitation and handle only joins with default strategy, but it's maybe too much to remember for the user ?
    // + Why penalize the user for supplying a custom partitioner !

    // Update1: there is an api KStream.repartition() that we can use to repartition the right topic before joining. I think
    // this is a better solution, it is explicit, and forces the user to think about partitioning strategy and also he can
    // repartition either the left topic or the right topic

    // Q2: We could do the same thing for KStream-KTable and want to repartition the right topic, is it not possible today and why ?
    // -> If we want to repartition the right topic, we will have to read it as a stream first, then repartition, then converting
    // it to a KTable. Even if we had a .repartition in KTable I guess we would have the same resulting topology, the KTable will
    // not be backed as a stream (cf. section about when KTable is stored), the node will behave as a stream forwarding every record
    // downstream.

    // Q: When we perform an operation over KTable (filter, mapValues, ...) that returns a KTable, does kafka streams maintain
    //    an internal store after every operation ??
    //      -> No, one thing I didn't know is when reading a topic as a table, no store is created !

    // Q: When does kafka streams decide to write the KTable in a store ?
    //          -> A general answer would be when it needs to know the change on the value (i.e. the previous value) in a certain processor downstream
    // Q: Does a GroupBy on a KTable write a store ?
    //    -> Yes, aggregation on KTable need a subtractor to handle deletions,
    //          and the subtractor usually (always ?) needs to know the change on the value


    // ============== To search 2 -- partly answered
    // Q1. Why would we need a KTable-GlobalKT join, is there a use case where KStream-GlobalKT won't do the job ?
    // Q2. For KStream-KTable if we input values into the topic of KTable, do we have updated outputs, or it is only the stream
    // which drives the join ? --> According to docs only kStream triggers the join, SO this is an example use case for 1.
    // Q3. Same question for KStream-GlobalKTable ? --> Same answer
    // Update: I think the use cases of KTable-GlobalKTable can be covered by KTable-KTable by repartitioning the right KTable
    // to have the same nb of partitions and same partitioning strategy.
    // One problem to this is when having a 10.000.000 (left) to 50 records (right), repartitioning is a waste of time and storage.

    streams
      .stream("input", Consumed.`with`(Serdes.String(), Serdes.String()))
      .process(
        new ProcessorSupplier[String, String, String, String]() {
          override def get(): Processor[String, String, String, String] = {
            new MyInputProcessor()
          }
        })
      .to("output", Produced.`with`(Serdes.String(), Serdes.String()))

    streams.build
  }

  private def kTableTopology: Topology = {
    val streams = new StreamsBuilder

    streams
      .table("input", Consumed.`with`(Serdes.String(), Serdes.String()))
      .filter((_, value) => value != "foo")
      .filter((_, value) => value != "bar")
      .filter((key, _) => key != "invalid")
      .toStream
      .to("output", Produced.`with`(Serdes.String(), Serdes.String()))

    streams.build()
  }

  test("test KTable topology - to remove") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(kTableTopology, props)
    val inputTopic = testDriver.createInputTopic("input", new StringSerializer, new StringSerializer)
    val outputTopic = testDriver.createOutputTopic("output", new StringDeserializer, new StringDeserializer)

    println(kTableTopology.describe())

    inputTopic.pipeInput("key1", "value1")

    readOutputTopic(outputTopic)
  }

  private def kTableGroupByTopology: Topology = {
    val streams = new StreamsBuilder()

    // Note: this topology leads to two internal state stores !! one for input and one for aggregations

    // Q1: Why in this topology kafka-streams needs an input store ?
    // -> Because the aggregation needs to know the change on the record (e.g. if a key got deleted from the upstream KTable)
    // In this case the aggregation should remove it using the aggregation's substractor function (for count it's doing a -1)

    // Q2: Why kafka-streams doesn't specify the processor that needs a serde ?
    // -> It should !

    // TODO: how does kafka streams correctly handle deletion of records from a table in the count ? And how does it send
    //  A "Change" without the user noticing it in the external interface ? In which step this change is sent ?

    val selector: KeyValueMapper[String, String, KeyValue[String, String]] = (key, value) => new KeyValue(value, key)

    streams
      .table("input", Consumed.`with`(Serdes.String(), Serdes.String()))
      .groupBy(selector, Grouped.`with`(Serdes.String(), Serdes.String())) // this gets translated to a SELECT node in the topology
      .count()
      .toStream()
      .to("output", Produced.`with`(Serdes.String(), Serdes.Long()))

    streams.build()
  }

  test("test KTable groupBy topology - to remove") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    println(kTableGroupByTopology.describe())

    val testDriver: TopologyTestDriver = new TopologyTestDriver(kTableGroupByTopology, props)
    val inputTopic = testDriver.createInputTopic("input", new StringSerializer, new StringSerializer)
    val outputTopic = testDriver.createOutputTopic("output", new StringDeserializer, new LongDeserializer)

    inputTopic.pipeInput("key1", "value1")
    inputTopic.pipeInput("key2", "value1")
    inputTopic.pipeInput(new TestRecord[String, String]("key1", null))

    readOutputTopic(outputTopic)
  }

  private def repartitionTopology: Topology = {
    val streams = new StreamsBuilder()

    streams
      .stream("input", Consumed.`with`(Serdes.String(), Serdes.String()))
      .repartition()
      .toTable()
      .toStream()
      .to("output", Produced.`with`(Serdes.String(), Serdes.String()))

    streams.build()
  }

  test("repartition") {
    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    println(repartitionTopology.describe())
  }


  /*

  Study join KTable-GlobalKTable
  - There is no problem with restarting a node, we will be reading from the next offset and not replaying all the global topic
  - A problem is during a rebalance, we will have to restore all the globalKTable. But I guess this is not a problem, since we
  do it for KTable as well ? For KTable-KTable join, if a node is rebalanced, we do the difference between restoring old store records
  and new records to forward downstream, no ?
      -> For KTable, restoration is done from the changelog, and we continue reading from the source topic from the current offset.
      This current offset is unique per application, as only one task is responsible of processing that partition.
            -> However, using Topology optimization, the source topic is the same as the changelog
            // TODO: if we read the source as a changelog we should stop restoring at the current offset, and then resume processing
                // from that offset, however usual changelog restoration restores all the topic, do we do such logic to stop at the current offset ?
                // because otherwise we will be processing the last records twice !! To test & Check KAFKA-5581 PR !
      -> For GlobalKTable, I don't think there is a changelog (to verify), we just restore the whole source topic.

  - From the global source topic point of view, how is each thread able to read the next offset since the partition is read from
  multiple tasks and thus the usual notion of offset is not supported here ? For a stream task, I assume it does not need to
  give the offset to read from, the broker should know it ? For the global consumer, does it give the offset to read from each time ?
  (I think from code PoV we should check whether streamTasks declare the same consumerGroupId and global threads different ones)
    - It seems that group.id is mandatory in consumer API, to what group.id do global threads assign themselves ?
    - "You should always configure group.id unless you are using the simple assignment API and you don’t need to store offsets in Kafka."

   */

  private def globalTableTopology: Topology = {
    val streams = new StreamsBuilder()

    val globalTable = streams.globalTable("global", Consumed.`with`(Serdes.String(), Serdes.String()))

    val keySelector: KeyValueMapper[String, String, String] = (key: String, value: String) => key

    streams
      .stream("input", Consumed.`with`(Serdes.String(), Serdes.String()))
      .join(globalTable, keySelector, (value1: String, value2: String) => value1 + value2)
      .to("output", Produced.`with`(Serdes.String(), Serdes.String()))

    streams.build()
  }

  test("globalTable") {
    println(globalTableTopology.describe())
  }

  test("6035") {
    val topology = {
      val streams = new StreamsBuilder()

      streams
        .stream("input", Consumed.`with`(Serdes.String(), Serdes.String()))
        .groupBy((key, value) => value)
        .count
        .toStream()
        .to("output", Produced.`with`(Serdes.String(), Serdes.Long()))

      streams.build()
    }

    println(topology.describe())
  }

  test("read/write GlobalKTable") {
    val topology = {
      val streams = new StreamsBuilder()

      val table = streams.globalTable("global", Consumed.`with`(Serdes.String(), Serdes.String()))

      /*
            streams
              .stream("input", Consumed.`with`(Serdes.String(), Serdes.String()))
              .peek((key, value) => println(s"received $key, $value"))
              .map((key, value) => table.range().toList.contains(key))
              // TODO Why inside process we can't define any function (instead of having to go by ProcessorSupplier)
              // TODO-2 Why not having some predefined processors that access the store we don't having to create a processorSupplier
              //  with init() and process() - e.g streams.process((key, value) => globalTable.contains(key))
              //  or the more realistic way streams.process((key, value) => new ContainsKey(globalStore, key))
              //  or streams.exists(store, (key, value) => value.keyToSearch) -> returns true or false as value
              //  deduplication can be much simplified with this new operator, where we just chain .exists and .filter
              .to("global")
      */
      streams.build()
    }

    val props = new Properties()
    props.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/")
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app")

    val testDriver: TopologyTestDriver = new TopologyTestDriver(topology, props)
    val inputTopic = testDriver.createInputTopic("input", new StringSerializer, new StringSerializer)
    val globalTopic = testDriver.createInputTopic("global", new StringSerializer, new StringSerializer)
    val outputTopic = testDriver.createOutputTopic("global", new StringDeserializer, new StringDeserializer)

    inputTopic.pipeInput("input1", "value1")
    // globalTopic.pipeInput("global1", "value1")

    readOutputTopic(outputTopic)
  }


  test("window store study") {
    class DedupProcessor extends Processor[String, String, String, String] {

      var store: WindowStore[String, String] = null
      var context: ProcessorContext[String, String] = null

      override def init(context: ProcessorContext[String, String]): Unit = {
        store = context.getStateStore("my-window-store")
        this.context = context
      }

      override def process(record: Record[String, String]): Unit = {
        val iterator = store.fetchAll(record.timestamp() - 10000L, record.timestamp() + 10000L)
        while (iterator.hasNext) {
          val keyValue = iterator.next()
          println(s"found record: ${Instant.ofEpochMilli(keyValue.key.window().start())}-${keyValue.value}")
        }
        println()
        iterator.close()
        store.put(record.key(), record.value(), record.timestamp())
      }
    }

    val streams = new StreamsBuilder()

    val topology = {
      streams.addStateStore(
        Stores.windowStoreBuilder(
          Stores.inMemoryWindowStore(
            "my-window-store",
            Duration.ofSeconds(10),
            Duration.ofSeconds(10),
            true
          ),
          Serdes.String(),
          Serdes.String()
        )
      )

      streams
        .stream("input", Consumed.`with`(Serdes.String(), Serdes.String()))
        .process(
          new ProcessorSupplier[String, String, String, String]() {
            override def get(): Processor[String, String, String, String] = {
              new DedupProcessor()
            }
          }, "my-window-store")
        // .peek((key, value) => println(s"record: $key-$value"))
        .to("output", Produced.`with`(Serdes.String(), Serdes.String()))

      streams.build
    }

    val testDriver: TopologyTestDriver = new TopologyTestDriver(topology)
    val inputTopic = testDriver.createInputTopic("input", new StringSerializer, new StringSerializer)
    val outputTopic = testDriver.createOutputTopic("output", new StringDeserializer, new StringDeserializer)

    val baseTime = new Date(2024 - 1900, 4, 3, 9, 30, 0).toInstant

    inputTopic.pipeInput("key1", "value1", baseTime)
    inputTopic.pipeInput("key1", "value2", baseTime)
    inputTopic.pipeInput("key1", "value3", baseTime.plusSeconds(5))

    readOutputTopic(outputTopic)

    /*
    Conclusions
    - Putting in a window store requires the window Start, if that timestamp does not correspond to any existing window start
    a new window is created.
     */
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
