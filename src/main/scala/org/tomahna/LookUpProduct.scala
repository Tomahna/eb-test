package org.tomahna

import java.io.FileWriter

import org.apache.spark.rdd.RDD

case class LookUpProduct(itemId: String, itemIdAsLong: Long)

object LookUpProduct {
  def itemLookUpRDD(ratings: RDD[Rating]): RDD[LookUpProduct] =
    ratings
      .map(_.itemId)
      .distinct()
      .zipWithIndex()
      .map((LookUpProduct.apply _).tupled)
      .persist()

  def save(items: RDD[LookUpProduct], file: String): Unit = {
    val fw = new FileWriter(file, true)
    try {
      items
        .map(i => s"${i.itemId},${i.itemIdAsLong}\n")
        .collect()
        .foreach(fw.write)
    } finally fw.close()
  }
}
