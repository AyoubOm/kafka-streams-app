package com.ayoubom.kafka

import org.apache.kafka.clients.producer.ProducerConfig

import java.time.Instant
import java.util.{Date, Properties, Timer, TimerTask}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.time.ZoneOffset

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

  // TODO: In case of tumbling windows without grace, Can kafka update a previous window even when a new new window starts ??!
  //  How can it be the case since the first window is already closed ?
  //   -> [TESTED] No, once a second window starts, the first window is no longer updated and records happening at that window are
  //    dropped

  val producer = new KafkaProducer[String, String](props)

  val baseTime = new Date(2024 - 1900, 3, 3, 9, 30, 0)
    .toInstant
    .plusSeconds(60)

  sendMessage(baseTime.plusSeconds(2))
  sendMessage(baseTime.plusSeconds(10))
  sendMessage(baseTime.plusSeconds(11))
  Thread.sleep(2000)
  sendMessage(baseTime.plusSeconds(8))

  private def sendMessage(timestamp: Instant): Unit = {
    producer
      .send(new ProducerRecord[String, String]("input", null, timestamp.toEpochMilli, s"key", "value"))
      .get()

    println(s"sent record - $timestamp")
  }

  private def schedule() = {
    val startTime = Instant.now()

    val startingSecond = startTime.atZone(ZoneOffset.UTC).getSecond

    new Timer().schedule(
      new TimerTask() {
        override def run(): Unit = {
          val timestamp = Instant.now()
          val second = timestamp.getEpochSecond % 60

          if (second != 58) {
            sendMessage(timestamp)
          }

          if (second == 2) {
            sendMessage(timestamp.minusSeconds(4))
          }
        }
      }, Date.from(Instant.now().plusSeconds(60 - startingSecond)), 1000L)

  }

}
