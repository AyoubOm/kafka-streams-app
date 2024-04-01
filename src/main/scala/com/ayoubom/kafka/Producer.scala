package com.ayoubom.kafka

import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

object Producer extends App {

  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

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
  for (_ <- 0 until 10) {
    producer
      .send(new ProducerRecord[String, String]("input", "myKey", "myValue"))
      .get()
  }

}
