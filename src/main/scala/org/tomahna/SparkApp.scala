package org.tomahna

import org.apache.spark.{SparkConf, SparkContext}

object SparkApp extends App {
  @transient lazy val conf: SparkConf =
    new SparkConf().setMaster("local").setAppName("eb")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  override def main(args: Array[String]): Unit = {
    val ratings = sc.textFile(args.head).flatMap(Rating.fromCSV).cache()
    val lookUpProduct = LookUpProduct.itemLookUpRDD(ratings)
    val lookUpUser = LookUpUser.userLookUpRDD(ratings)

    LookUpProduct.save(lookUpProduct, "lookup_product.csv")
    LookUpUser.save(lookUpUser, "lookup_user.csv")
  }
}
