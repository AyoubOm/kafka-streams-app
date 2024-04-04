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

  val baseTime = new Date(2024 - 1900, 3, 14, 9, 30, 0)
    .toInstant

  sendMessage("input1", baseTime.plusSeconds(2), "key1", "2")

  sendMessage("input2", baseTime.plusSeconds(4), "key1", "4")

  sendMessage("input1", baseTime.plusSeconds(5), "key1", "5")

  sendMessage("input2", baseTime.plusSeconds(3600), "key1", "3600")

  Thread.sleep(5000)

  sendMessage("input1", baseTime.plusSeconds(24), "key1", "24")


  sendMessage("input2", baseTime.plusSeconds(8), "key1", "8") // why not ignored ?
    // => it is only not ignored when stream of input1 is not advanced, when the window that matches with this value is not yet closed


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