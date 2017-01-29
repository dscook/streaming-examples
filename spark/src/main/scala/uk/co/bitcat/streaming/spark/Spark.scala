package uk.co.bitcat.streaming.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object Spark {

  case class Reading(time: String, pollution: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")

    // Setting the batch interval over which we perform our pollution average calculation
    val streamingContext = new StreamingContext(conf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pollution_reader",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Creating a stream to read from Kafka
    val topics = Array("pollution")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // Calculate the pollution average over the last interval
    stream.foreachRDD { rdd =>

      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val row = rdd
        .map(_.value.split(","))
        .map(attributes => Reading(attributes(0), attributes(1).trim.toInt))
        .toDF()
        .agg(avg($"pollution") as "pollutionAverage")
        .filter($"pollutionAverage" > 80.0)
        .collect();

      if (row.length > 0) {
        // Raise alert. For instance call to REST endpoint, email, SMS and so on ..
        println(row(0))
      }

    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}