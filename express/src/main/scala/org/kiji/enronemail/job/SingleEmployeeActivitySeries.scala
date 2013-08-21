package org.kiji.enronemail.job

import com.twitter.scalding.{Tsv, Args}
import org.kiji.express.flow._
import org.slf4j.LoggerFactory
import org.kiji.express.{Cell, KijiSlice, EntityId}
import org.joda.time.DateTime

class SingleEmployeeActivitySeries(args: Args) extends KijiJob(args) {
  lazy val log = LoggerFactory.getLogger(getClass)
  // Conveniently get the correct path separator for the platform we're running on
  val sep = java.io.File.separatorChar

  // Parse arguments
  val inputUri: String = args("input")// This should be the 'employee' table
  val outputUri: String = args("output")


  val emailsByHour = KijiInput(inputUri)(Map(Column("sent_messages:to").withPaging(10) -> 'to))
    .map('to -> 'to){
      toSlice: KijiSlice[CharSequence] => // now, each sender has a tuple associated with every
      // email they have sent.
      toSlice.cells}
    .map('to -> 'time){
      cell: Cell[CharSequence] =>
      val dateTime = new DateTime(cell.version)
      dateTime.hourOfDay.get}// the hour of the day this email was sent, as an Int.
     .discard('to)

  val emailCountByHour = emailsByHour.groupBy('time) { group => group.size } // count how many emails get sent an hour

  emailCountByHour.write(Tsv(outputUri))
}
