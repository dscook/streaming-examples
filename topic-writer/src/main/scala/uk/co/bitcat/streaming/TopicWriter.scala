package uk.co.bitcat.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object TopicWriter {
  def main(args: Array[String]) {
    val source = Source.fromResource("readings.csv")
    val lines : Iterator[String] = source.getLines

    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "Pollution")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    for (line <- lines) {
      val data = new ProducerRecord[String, String]("pollution", line)
      producer.send(data)
    }

    producer.close
    source.close
  }
}