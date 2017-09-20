package org.tomahna

import java.io.FileWriter

import org.apache.spark.rdd.RDD

object LookUpUser {
  def userLookUpRDD(ratings: RDD[Rating]): RDD[(String, Long)] =
    ratings.map(_.userId).distinct().zipWithUniqueId().cache()

  def save(users: RDD[(String, Long)], file: String): Unit = {
    val fw = new FileWriter(file, true)
    try {
      users.collect().foreach(u => fw.write(s"{${u._1},${u._2}\n"))
    } finally fw.close()
  }
}
