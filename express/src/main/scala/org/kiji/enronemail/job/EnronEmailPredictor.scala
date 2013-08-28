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
    .filter('timestamps) { timestamps: List[Long] => timestamps.size > 1}
    .map('timestamps -> 'avgDelta) {
    timestamps: List[Long] =>

      val deltas = for (i <- 1 until timestamps.size) yield {
        timestamps(i - 1) - timestamps(i)
      }

      deltas.sum / deltas.size
  }
    .project('first, 'second, 'avgDelta)
    .write(Tsv(outputUri + sep + "prediction-table-enron"))
}
