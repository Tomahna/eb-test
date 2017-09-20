package org.tomahna

import org.apache.spark.rdd.RDD

object KeyJoin {
  implicit class KeyJoiner[T](rdd: RDD[((String, String), T)]) {
    def joinWithIndex(implicit p: RDD[LookUpProduct],
                      u: RDD[LookUpUser]): RDD[((Long, Long), T)] = {
      val userBase = u.map(u => (u.userId, u.userIdAsLong))
      val productBase = p.map(i => (i.itemId, i.itemIdAsLong))

      rdd
        .map { case ((uId, iId), r) => (uId, (iId, r)) }
        .join(userBase)
        .map { case (_, ((iId, v), uId)) => (iId, (uId, v)) }
        .join(productBase)
        .map { case (_, ((uId, v), iId)) => ((uId, iId), v) }
    }
  }
}
