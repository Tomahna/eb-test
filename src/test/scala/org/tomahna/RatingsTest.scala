package org.tomahna

import org.apache.spark.rdd.RDD
import org.tomahna.SparkApp._
import org.scalatest.{FlatSpec, Matchers}

class RatingsTest extends FlatSpec with Matchers {
  "Aggregate" should "be unique by (user/product)" in {
    val ratings = sc.textFile("xag.csv").flatMap(Rating.fromCSV).cache()
    implicit val lookUpProduct: RDD[LookUpProduct] =
      LookUpProduct.itemLookUpRDD(ratings)
    implicit val lookUpUser: RDD[LookUpUser] =
      LookUpUser.userLookUpRDD(ratings)
    val latestRating = Ratings.latestDate(ratings)
    val aggregates = Ratings.aggregate(ratings, latestRating).persist()

    val size = aggregates.aggregate(0l)((n, _) => n + 1, _ + _)
    val groupedSize = aggregates
      .map(r => r._1)
      .distinct()
      .aggregate(0l)((n, _) => n + 1, _ + _)
    size should be(groupedSize)
  }
}
