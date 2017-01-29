package uk.co.bitcat.streaming.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object Flink {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val counts = text.flatMap {
      _.toLowerCase.split("\\W+")
    }
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)

    counts.print()

    env.execute("WordCount Example")
  }

}
