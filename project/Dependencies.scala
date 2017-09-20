import sbt._

object Dependencies {
  val scalaTest: ModuleID = "org.scalatest" %% "scalatest" % "3.0.3"

  val spark: ModuleID = "org.apache.spark" %% "spark-core" % "2.2.0"
}
