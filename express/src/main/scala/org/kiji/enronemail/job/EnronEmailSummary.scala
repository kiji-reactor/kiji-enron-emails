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

class EnronEmailSummary(args: Args) extends KijiJob(args) {

  object ord extends Ordering[(Long, String)] {
    def compare(a: (Long, String), b: (Long, String)) = a._1 compare b._1
  }

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
    .flatMapTo('from -> 'fromStr) { from: KijiSlice[String] =>
      from.cells.map(cell => cell.datum) }
    .groupBy('fromStr) { _.size('emailCount) }
//    .groupAll { _.sortWithTake(('emailCount, 'fromStr) -> 'top, 10)
//      { (t0: (Long, String), t1: (Long, String)) => t0._1 < t1._1 }
//      }
    .groupAll { _.sortedTake[(Long, String)](('emailCount, 'fromStr) -> 'top, 10) }
//    .flatten[(String, Long)]('top -> ('fromStr, 'emailCount))
//    .discard('top)
    .write(Tsv(outputUri + sep + "top-senders-enron"))

  // Configure another input source -- The from and to columns are required this time to create pairs of correspondents
//  KijiInput(inputUri)(Map(Column("info:from") -> 'sender, Column("info:to") -> 'recipients))
//    // 'recipients actually contains comma separated emails since there can be more than one recipient
//    // we need to "explode" this field so we have an email sent from each sender to each recipient
//    // Just in case our input data wasn't completely sanitized we'll do some trimming and make everything lowercase
//    // before we do the split.
//    // flatMap takes in a field and maps it to a new field containing a collection of values for each input value
//    .flatMap('recipients -> 'recipients) { recipients: KijiSlice[String] => recipients.cells.map(cell => cell.datum.trim.toLowerCase.split(",")) }
//    // Now we can do a proper groupBy and output the size of each group (a count)
//    .groupBy('sender, 'recipients) { _.size }
//    .write(Tsv(outputUri + sep + "top-correspondents-enron"))
}
