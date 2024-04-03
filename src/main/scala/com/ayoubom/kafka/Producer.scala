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

  val producer = new KafkaProducer[String, String](props)

  val baseTime = new Date(2024 - 1900, 3, 6, 9, 30, 0)
    .toInstant

  /*
  sendMessage("input1", baseTime.plusSeconds(5), "key1", "5")

  Thread.sleep(2000)

  sendMessage("input2", baseTime.plusSeconds(25), "key2","25") // advances stream Time

  Thread.sleep(2000)

  sendMessage("input2", baseTime.plusSeconds(6), "key1","6") // outputs sthg ?
   */

  sendMessage("input1", baseTime.plusSeconds(1), "key1", "1")
  sendMessage("input2", baseTime.plusSeconds(1), "key1", "1")




  private def sendMessage(topic: String, timestamp: Instant, key: String = "key1", value: String = "value"): Unit = {
    producer
      .send(new ProducerRecord[String, String](topic, null, timestamp.toEpochMilli, key, value))
      .get()

    println(s"sent record - $topic - $timestamp")
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
            sendMessage("input", timestamp)
          }

          if (second == 2) {
            sendMessage("input", timestamp.minusSeconds(4))
          }
        }
      }, Date.from(Instant.now().plusSeconds(60 - startingSecond)), 1000L)

  }

}
