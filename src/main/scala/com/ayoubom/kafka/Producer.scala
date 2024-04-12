package com.ayoubom.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Date, Properties}

object Producer extends App {

  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
  props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE)
  props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  props.put(ProducerConfig.LINGER_MS_CONFIG, 1)

  val producer = new KafkaProducer[String, String](props)

  val baseTime = new Date(2024 - 1900, 3, 15, 9, 30, 0)
    .toInstant

  sendMessage("input", "key1", 4)

 Thread.sleep(5000)

  sendMessage("input", "key1", 12)

 Thread.sleep(3000)

  sendMessage("input", "key1", 22)




//  sendMessage("input", "key1", 30)


  private def sendMessage(topic: String, key: String = "key1", value: Int): Unit = {
    val timestamp = baseTime.plusSeconds(value)
    producer
      .send(new ProducerRecord[String, String](topic, null, timestamp.toEpochMilli, key, value.toString))
      .get()

    println(s"sent record - $topic - $timestamp")
  }

}
