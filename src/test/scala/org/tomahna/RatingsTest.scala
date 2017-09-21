package org.tomahna

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.tomahna.SparkApp._
import org.scalatest.{FlatSpec, Matchers}
import org.tomahna.RDDImplicits._

class RatingsTest extends FlatSpec with Matchers {
  "Aggregate" should "be unique by (user/product)" in {
    val ratings = sc.textFile("xag.csv").flatMap(Rating.fromCSV).cache()
    implicit val lookUpProduct: RDD[LookUpProduct] =
      LookUpProduct.itemLookUpRDD(ratings)
    implicit val lookUpUser: RDD[LookUpUser] =
      LookUpUser.userLookUpRDD(ratings)
    val latestRating = Ratings.latestDate(ratings)
    val aggregates = Ratings.aggregate(ratings, latestRating).persist()

    val size = aggregates.count()
    val groupedSize = aggregates.map(r => r._1).distinct().count()
    size should be(groupedSize)
  }
  it should "apply 0.95 penalty per day" in {
    val ratings = sc.parallelize(
      Seq(
        Rating("John", "toothbrush", 1f, LocalDate.of(2017, 10, 12)),
        Rating("John", "toothbrush", 1f, LocalDate.of(2017, 10, 11))
      ))
    implicit val lookUpProduct: RDD[LookUpProduct] = sc.parallelize(
      Seq(LookUpProduct("toothbrush", 1))
    )
    implicit val lookUpUser: RDD[LookUpUser] = sc.parallelize(
      Seq(LookUpUser("John", 1))
    )
    val latestRating = Ratings.latestDate(ratings)
    val aggregates: RDD[((Long, Long), Float)] =
      Ratings.aggregate(ratings, latestRating).persist()

    aggregates should contain(((1, 1), 1.95f))
  }
  it should "filter ratings < 0.01" in {
    val ratings = sc.parallelize(
      Seq(
        Rating("John", "toothbrush", 1f, LocalDate.of(2017, 10, 12)),
        Rating("John", "toothbrush", 0.01f, LocalDate.of(2017, 10, 11))
      ))
    implicit val lookUpProduct: RDD[LookUpProduct] = sc.parallelize(
      Seq(LookUpProduct("toothbrush", 1))
    )
    implicit val lookUpUser: RDD[LookUpUser] = sc.parallelize(
      Seq(LookUpUser("John", 1))
    )
    val latestRating = Ratings.latestDate(ratings)
    val aggregates: RDD[((Long, Long), Float)] =
      Ratings.aggregate(ratings, latestRating).persist()

    aggregates should contain(((1, 1), 1f))
  }
}
