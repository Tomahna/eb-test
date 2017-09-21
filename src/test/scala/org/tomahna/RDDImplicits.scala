package org.tomahna

import org.apache.spark.rdd.RDD
import org.scalatest.enablers.Containing

object RDDImplicits {
  implicit def rddContaining[T]: Containing[RDD[T]] = new Containing[RDD[T]](){
    override def contains(container: RDD[T], element: Any): Boolean =
      container.filter(_ == element).count() > 0

    override def containsOneOf(container: RDD[T], elements: Seq[Any]): Boolean =
      container.filter(elements contains _).count() > 0

    override def containsNoneOf(container: RDD[T], elements: Seq[Any]): Boolean =
      container.filter(elements contains _).count() == 0
  }
}
