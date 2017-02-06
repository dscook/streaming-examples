package uk.co.bitcat.streaming.flink

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.util.Collector
import uk.co.bitcat.streaming.flink.domain.{Measurement, MeasurementSchema}
import uk.co.bitcat.streaming.flink.watermark.TwoSecondDelayWatermark

object Flink {

  case class Reading(time: String, pollution: Int)

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink_consumer")

    env
      .addSource(new FlinkKafkaConsumer09[Measurement]("pollution", new MeasurementSchema(), properties))
      .assignTimestampsAndWatermarks(new TwoSecondDelayWatermark())
      .timeWindowAll(Time.seconds(10))
      .apply(
        (0L, 0.0, 0), // (Window End Time, To Store Mean, Count)
        (acc: (Long, Double, Int), m: Measurement) => { (0L, acc._2 + m.pollution, acc._3 + 1) },
        ( window: TimeWindow,
          counts: Iterable[(Long, Double, Int)],
          out: Collector[(Long, Double, Int)] ) =>
        {
          val count = counts.iterator.next()
          out.collect((window.getEnd, count._2/count._3, count._3))
        }
      )
      .filter(_._2 > 75.0)
      .print()  // Replace with call to custom sink to raise alert for pollution level

    env.execute()
  }

}
