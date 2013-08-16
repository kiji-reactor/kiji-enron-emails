/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.enronemail.job

import org.slf4j.LoggerFactory
import com.twitter.scalding.{TupleConverter, Tsv, Args}
import org.kiji.express.flow.{KijiJob, Column, KijiInput}
import org.kiji.express.KijiSlice

class EnronEmailSummaryCompleted(args: Args) extends KijiJob(args) {

  lazy val log = LoggerFactory.getLogger(getClass)

  // Conveniently get the correct path separator for the platform we're running on
  val sep = java.io.File.separatorChar


  // Parse arguments
  val inputUri: String = args("input")
  val outputUri: String = args("output")

  /**
   * Configure a pipe to compute top 10 email senders
   */
  // Configure an input source -- The "from" column in our HBase table mapped to the 'from field in our pipe
  KijiInput(inputUri)(Map(Column("info:from") -> 'from))
    // KijiInput provides a KijiSlice which is a collection of cells for the requested column(s)
    // We are interested in the value of each cell, which is a string representing a from email address, so
    // we do a flat map to store the strings in the 'fromStr field
    .flatMapTo('from -> 'fromStr) { from: KijiSlice[String] =>
      from.cells.map(cell => cell.datum) }
    // Here we group by each sender. The group by gives us a GroupBuilder which allows us to call reduce-type
    // functions on each group. Here we call size to compute the size of each group. We optionally store it to
    // the named tuple field 'emailCount
    .groupBy('fromStr) { _.size('emailCount) }
    // Since 'emailCount didn't exist when the first groupBy was called, we must do our sorting in a separate
    // call to a grouping function. But... because we already did our grouping, we can call groupAll which
    // will simply place everything into one group. We do a reverse sort (descending) by emailCount
    .groupAll { _.sortBy('emailCount).reverse }
    // Now since everything is in the correct order, we can limit our output to 10 values
    .limit(10)
    // And write it to an output file in HDFS. We could also write to a KijiTable or a number of other sinks
    .write(Tsv(outputUri + sep + "top-senders-enron"))

  /**
   * Configure a pipe to compute top 10 correspondents
   */
  // Configure another input source -- The from and to columns are required this time to create pairs of correspondents
  KijiInput(inputUri)(Map(Column("info:from") -> 'fromColumn, Column("info:to") -> 'toColumn))
    // 'recipients actually contains comma separated emails since there can be more than one recipient
    // we need to "explode" this field so we have an email sent from each sender to each recipient
    // Just in case our input data wasn't completely sanitized we'll do some trimming and make everything lowercase
    // before we do the split.
    .flatMap('toColumn -> 'recipients) { recipients: KijiSlice[String] => recipients.cells.map(cell => cell.datum) }
    .flatMap('fromColumn -> 'sender) { sender: KijiSlice[String] => sender.cells.map(cell => cell.datum.trim.toLowerCase) }
    .flatMap('recipients -> 'recipient) { recipients: String => recipients.split(",").map(_.trim.toLowerCase) }
    .discard('toColumn, 'fromColumn, 'recipients)
    // Now we can do a proper groupBy and output the size of each group (a count)
    .groupBy('sender, 'recipient) { _.size('emailCount) }
    .groupAll { _.sortBy('emailCount).reverse }
    .limit(10)
    .write(Tsv(outputUri + sep + "top-correspondents-enron"))
}
