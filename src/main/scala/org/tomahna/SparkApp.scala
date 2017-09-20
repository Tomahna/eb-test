package org.tomahna

import org.apache.spark.{SparkConf, SparkContext}

object SparkApp extends App {
  @transient lazy val conf: SparkConf =
    new SparkConf().setMaster("local").setAppName("eb")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  override def main(args: Array[String]): Unit = {
    val lines = sc.textFile(args.head).flatMap(Rating.fromCSV)
  }
}
