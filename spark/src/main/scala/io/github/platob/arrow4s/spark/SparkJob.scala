
package io.github.platob.arrow4s.spark

import org.apache.spark.sql.SparkSession
import io.github.platob.arrow4s.core.HelloCore

object SparkJob {
  def run(appName: String = "arrow4s-spark-demo"): String = {
    val spark = SparkSession.builder().appName(appName).master("local[*]").getOrCreate()
    try {
      val greeting = HelloCore.hi("spark")
      import spark.implicits._
      val ds = Seq(1, 2, 3).toDS
      val sum = ds.reduce(_ + _)
      s"$greeting Sum=$sum"
    } finally {
      spark.stop()
    }
  }
}
