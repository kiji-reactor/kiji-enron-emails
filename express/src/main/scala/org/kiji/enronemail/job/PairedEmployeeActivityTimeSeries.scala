package org.kiji.enronemail.job

import com.twitter.scalding.{Tsv, Args}
import org.kiji.express.flow._
import org.slf4j.LoggerFactory
import org.scala_tools.time.Imports._
import org.kiji.express.{EntityId, KijiSlice, Cell}


class PairedEmployeeActivityTimeSeries(args: Args) extends KijiJob(args) {
  lazy val log = LoggerFactory.getLogger(getClass)

  // Parse arguments
  val inputUri: String = args("input")// This should be the 'employee' table
  val outputUri: String = args("output")

  val fromToTimeTriples = KijiInput(inputUri)(Map(Column("info:to") -> 'to))
    .map('entityId -> 'fromId){eId: EntityId => eId(0)}.discard('entityID) // retrieve the email
      // identifier of the sender
    .flatMap('to -> 'to){
      toSlice: KijiSlice[CharSequence] => // now, each sender has a tuple for every  email they have sent.
          toSlice.cells
    }.map('to -> ('toId, 'hourOfDay)){
      cell: Cell[CharSequence] =>
        val dateTime = new DateTime(cell.version)
        (cell.datum, dateTime.hourOfDay())
    }.discard('to)
  fromToTimeTriples.groupBy('from, 'to, 'hourOfDay){group => group.size}
      .write(Tsv(outputUri))

}
