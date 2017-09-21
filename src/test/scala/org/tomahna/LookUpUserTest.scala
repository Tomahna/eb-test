package org.tomahna

import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}
import org.tomahna.SparkApp._
import org.tomahna.RDDImplicits._

class LookUpUserTest extends FlatSpec with Matchers {
  "LookUpUser" should "should contains consecutive ids" in {
    val ratings = sc.textFile("xag.csv").flatMap(Rating.fromCSV).cache()
    val lookUpUser: RDD[LookUpUser] = LookUpUser.userLookUpRDD(ratings)

    val ids = lookUpUser.map(u => u.userIdAsLong).collect().sorted
    ids.diff(0l to ids.last) shouldBe empty
  }

  "LookUpUser" should "have id 0" in {
    val ratings = sc.textFile("xag.csv").flatMap(Rating.fromCSV).cache()
    val lookUpUser: RDD[LookUpUser] = LookUpUser.userLookUpRDD(ratings)

    lookUpUser.map(_.userIdAsLong) should contain(0)
  }
}
