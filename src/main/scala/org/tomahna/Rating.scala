package org.tomahna

import java.util.Date

case class Rating(
    userId: String,
    itemId: String,
    rating: Float,
    timestamp: Date
)

object Rating {
  def fromCSV(row: String): Option[Rating] = row.split(',') match {
    case Array(uId, iId, r, t) => Some(Rating(uId, iId, r.toFloat, new Date(t.toLong * 1000)))
    case _ => None
  }
}
