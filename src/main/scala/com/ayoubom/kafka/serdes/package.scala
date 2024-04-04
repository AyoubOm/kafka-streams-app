package com.ayoubom.kafka

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.StreamJoined
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier

package object serdes {

  private def storeSupplier(storeName: String) =
    new RocksDbWindowBytesStoreSupplier(
      storeName,
      20000,
      20000,
      10000 + 10000, // afterMs + beforeMs
      true,
      true)


  val streamJoined: StreamJoined[String, String, String] =
    StreamJoined.`with`(storeSupplier("this"), storeSupplier("other"))
    .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()).withOtherValueSerde(Serdes.String())

}
