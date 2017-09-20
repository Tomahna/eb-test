package org.tomahna

import java.time._

case class Rating(
    userId: String,
    itemId: String,
    rating: Float,
    timestamp: LocalDate
)

object Rating {
  def fromCSV(row: String): Option[Rating] = row.split(',') match {
    case Array(uId, iId, r, t) => Some(Rating(uId, iId, r.toFloat, toDate(t)))
    case _ => None
  }

  private def toDate(epoch: String): LocalDate =
    Instant.ofEpochMilli(epoch.toLong).atZone(ZoneId.systemDefault).toLocalDate
}
