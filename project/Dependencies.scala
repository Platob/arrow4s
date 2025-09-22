
import sbt._

object Dependencies {
  object V {
    val scala212 = "2.12.18"
    val scala213 = "2.13.14"

    val munit        = "1.0.0"
    val spark        = "3.5.1" // 3.5 supports 2.12 & 2.13
    val arrow        = "18.3.0"
  }

  val test = Seq(
    "org.scalameta" %% "munit" % V.munit % Test
  )

  val arrow = Seq(
    // Apache Arrow
    "org.apache.arrow" % "arrow-vector" % V.arrow,
    "org.apache.arrow" % "arrow-memory-netty" % V.arrow,
    "org.apache.arrow" % "arrow-format" % V.arrow
  )

  val sparkProvided = Seq(
    "org.apache.spark" %% "spark-sql"  % V.spark % Provided,
    "org.apache.spark" %% "spark-core" % V.spark % Provided
  )
}
