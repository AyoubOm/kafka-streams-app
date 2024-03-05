package com.ayoubom.kafka

import com.ayoubom.kafka.JsonSerde.{Encoding, JsonObjMapper}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.reflect.ClassTag

class JsonSerde[T >:Null : Manifest] extends Serde[T] {

  override def serializer(): Serializer[T] = new JsonSerializer[T]

  override def deserializer(): Deserializer[T] = new JsonDeserializer[T]
}

class JsonSerializer[T >:Null : Manifest] extends Serializer[T] {
  override def serialize(topic: String, data: T): Array[Byte] = {
    if (data == null) null
    else JsonObjMapper.writeValueAsString(data).getBytes(Encoding)
  }
}

class JsonDeserializer[T >:Null : Manifest] extends Deserializer[T] {
  override def deserialize(s: String, data: Array[Byte]): T = {
    if (data == null) null
    else JsonObjMapper.readValue(data, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]
  }
}

object JsonSerde {
  val JsonObjMapper = new ObjectMapper()
  JsonObjMapper.registerModule(DefaultScalaModule)

  val Encoding = "UTF-8"
}

