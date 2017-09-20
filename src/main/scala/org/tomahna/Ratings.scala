package org.tomahna

import java.io.FileWriter
import java.time._
import java.time.temporal.ChronoUnit

import org.apache.spark.rdd.RDD
import KeyJoin._

object Ratings {
  def aggregate(ratings: RDD[Rating], mostRecent: LocalDate)(
      implicit userLookUp: RDD[LookUpUser],
      itemLookUp: RDD[LookUpProduct]): RDD[((Long, Long), Float)] =
    ratings
      .map(r => ((r.userId, r.itemId), (r.rating, r.timestamp)))
      .mapValues(v => v._1 * penalty(v._2, mostRecent))
      .filter(_._2 > 0.01f)
      .groupByKey()
      .mapValues(_.sum)
      .joinWithIndex

  def latestDate(ratings: RDD[Rating]): LocalDate =
    ratings.map(_.timestamp).max()

  def save(users: RDD[((Long, Long), Float)], file: String): Unit = {
    val fw = new FileWriter(file, true)
    try {
      users.map(a => s"${a._1._1},${a._1._2},${a._2}\n").collect().foreach(fw.write)
    } finally fw.close()
  }

  private def penalty(currentDate: LocalDate, mostRecent: LocalDate): Float =
    math.pow(0.95, daysBetween(currentDate, mostRecent)).toFloat

  private def daysBetween(d1: LocalDate, d2: LocalDate): Long =
    Period.between(d1, d2).get(ChronoUnit.DAYS)

  private implicit val localDateOrdering: Ordering[LocalDate] =
    new Ordering[LocalDate]() {
      override def compare(x: LocalDate, y: LocalDate): Int = x.compareTo(y)
    }
}
