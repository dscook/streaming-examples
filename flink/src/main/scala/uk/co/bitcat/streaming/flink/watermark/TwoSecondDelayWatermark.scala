package uk.co.bitcat.streaming.flink.watermark

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import uk.co.bitcat.streaming.flink.domain.Measurement

class TwoSecondDelayWatermark extends BoundedOutOfOrdernessTimestampExtractor[Measurement](Time.seconds(2)) {
  override def extractTimestamp(element: Measurement): Long = element.time
}