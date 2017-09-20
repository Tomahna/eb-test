package org.tomahna

import java.io.FileWriter

import org.apache.spark.rdd.RDD

case class LookUpUser(userId: String, userIdAsLong: Long)

object LookUpUser {
  def userLookUpRDD(ratings: RDD[Rating]): RDD[LookUpUser] =
    ratings
      .map(_.userId)
      .distinct()
      .zipWithUniqueId()
      .map((LookUpUser.apply _).tupled)
      .persist()

  def save(users: RDD[LookUpUser], file: String): Unit = {
    val fw = new FileWriter(file, true)
    try {
      users.map(u => s"${u.userId},${u.userIdAsLong}\n").collect().foreach(fw.write)
    } finally fw.close()
  }
}
