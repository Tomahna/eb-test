package org.tomahna

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkApp extends App {
  @transient lazy val conf: SparkConf =
    new SparkConf().setMaster("local[4]").setAppName("eb")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  override def main(args: Array[String]): Unit = {
    val ratings = sc.textFile(args.head).flatMap(Rating.fromCSV).cache()
    implicit val lookUpProduct: RDD[LookUpProduct] = LookUpProduct.itemLookUpRDD(ratings)
    implicit val lookUpUser: RDD[LookUpUser] = LookUpUser.userLookUpRDD(ratings)
    val latestRating = Ratings.latestDate(ratings)
    val aggregates = Ratings.aggregate(ratings, latestRating)

    LookUpProduct.save(lookUpProduct, "lookup_product.csv")
    LookUpUser.save(lookUpUser, "lookup_user.csv")
    Ratings.save(aggregates, "aggregating.csv")
  }
}
