package com.ayoubom.kafka.utils

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.HostInfo

object IQService {

  private val host = "localhost"
  private val port = 7070

  def startRestProxy(streams: KafkaStreams): IQueriesService = {
    val hostInfo = new HostInfo(host, port)
    val iqService = new IQueriesService(streams, hostInfo)
    iqService.start(port)
    iqService
  }

}
