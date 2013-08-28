package org.kiji.enronemail.job

import com.twitter.scalding.{Tsv, Args}
import org.kiji.express.flow.{Column, KijiInput, KijiJob}
import org.slf4j.LoggerFactory
import org.kiji.express.KijiSlice

class EnronEmailPredictor(args: Args) extends KijiJob(args) {

  lazy val log = LoggerFactory.getLogger(getClass)

  // Conveniently get the correct path separator for the platform we're running on
  val sep = java.io.File.separatorChar


  // Parse arguments
  val inputUri: String = args("input")
  val outputUri: String = args("output")

  KijiInput(inputUri)(Map(Column("info:from") -> 'from, Column("info:to") -> 'to, Column("info:date") -> 'timestamp))
    .mapTo(('from, 'to, 'timestamp) ->('from, 'to, 'timestamp)) {
    columns: (KijiSlice[String], KijiSlice[String], KijiSlice[Long]) =>
      val (fromColumn, toColumn, timestamp) = columns
      (fromColumn.getFirstValue(), toColumn.getFirstValue(), timestamp.getFirstValue())
  }
    .flatMap('to -> 'to) {
    recipients: String => recipients.split(",").map(_.trim.toLowerCase)
  }
    .map(('from, 'to) ->('first, 'second)) {
    tuple: (String, String) =>
      val (from, to) = tuple
      if (to < from) {
        (to, from)
      } else {
        (from, to)
      }
  }
    .groupBy('first, 'second) {
    _.toList[Long]('timestamp -> 'timestamps)
  }
    .map('timestamps -> 'deltas) {
    timestamps: List[Long] =>

      for (i <- 1 until timestamps.size) yield {
        timestamps(i - 1) - timestamps(i)
      }
  }
    .project('first, 'second, 'timestamps, 'deltas)
    .groupAll {
    _.sortWithTake[(String, String, List[Long], List[Long])](('first, 'second, 'timestamps, 'deltas) -> 'top, 10) {
      (t0: (String, String, List[Long], List[Long]), t1: (String, String, List[Long], List[Long])) =>
        t0._3.size > t1._3.size
    }
  }
    .flattenTo[(String,String, List[Long], List[Long])]('top -> ('first, 'second, 'timestamps, 'deltas))
    .map('timestamps -> 'timestamps) { timestamps: List[Long] => timestamps.reduceLeft[String] { (acc, n) =>
      acc + ", " + n }}
    .map('deltas -> 'deltas) { timestamps: List[Long] => timestamps.reduceLeft[String] { (acc, n) =>
    acc + ", " + n }}
    .write(Tsv(outputUri + sep + "top-correspondents-enron"))
}
