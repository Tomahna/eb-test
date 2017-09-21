package org.tomahna

import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}
import org.tomahna.SparkApp._

class LookUpUserTest extends FlatSpec with Matchers {
  "Aggregate" should "be unique by (user/product)" in {
    val ratings = sc.textFile("xag.csv").flatMap(Rating.fromCSV).cache()
    implicit val lookUpUser: RDD[LookUpUser] =
      LookUpUser.userLookUpRDD(ratings)

    val ids = lookUpUser.map(u => u.userIdAsLong).collect().sorted
    ids.diff(0l to ids.last) shouldBe empty
  }
}
