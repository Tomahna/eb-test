package org.tomahna

import java.io.FileWriter

import org.apache.spark.rdd.RDD

object LookUpProduct {
  def itemLookUpRDD(ratings: RDD[Rating]): RDD[(String, Long)] =
    ratings.map(_.itemId).distinct().zipWithUniqueId()

  def save(items: RDD[(String, Long)], file: String): Unit = {
    val fw = new FileWriter(file, true)
    try {
      items.collect().foreach(i => fw.write(s"${i._1},${i._2}"))
    } finally fw.close()
  }
}
