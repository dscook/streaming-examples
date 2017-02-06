package uk.co.bitcat.streaming.flink.domain

import java.time.Instant

import org.apache.flink.streaming.util.serialization.{AbstractDeserializationSchema, SerializationSchema}

case class Measurement(time: Long, pollution: Int)

class MeasurementSchema extends AbstractDeserializationSchema[Measurement] with SerializationSchema[Measurement] {

  override def deserialize(bytes: Array[Byte]): Measurement = {
    val tokens = new String(bytes).split(",")
    val timeSinceEpoch = Instant.parse(tokens(0).trim).toEpochMilli
    Measurement(timeSinceEpoch, tokens(1).trim.toInt)
  }

  override def serialize(element: Measurement): Array[Byte] = {
    val buffer = new StringBuilder()
    buffer ++= Instant.ofEpochMilli(element.time).toString
    buffer ++= ","
    buffer ++= element.pollution.toString
    buffer.toString.getBytes
  }

}