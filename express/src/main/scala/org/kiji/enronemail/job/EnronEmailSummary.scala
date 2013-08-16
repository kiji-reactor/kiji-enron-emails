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

  lazy val log = LoggerFactory.getLogger(getClass)

  // Conveniently get the correct path separator for the platform we're running on
  val sep = java.io.File.separatorChar


  // Parse arguments
  val inputUri: String = args("input")
  val outputUri: String = args("output")

  /**
   * Configure a pipe to compute top 10 email senders
   */
  val topSenders =
    // Configure an input source -- The "from" column in our HBase table mapped to the 'from field in our pipe
    KijiInput(inputUri)(Map(Column("info:from") -> 'from))

  topSenders.write(Tsv(outputUri + sep + "top-senders-enron"))
  /**
   * Configure a pipe to compute the top 10 most frequent corresponders
   */
  val topCorresponders =
    // Configure another input source -- The from and to columns are required this time to create pairs of correspondents
    KijiInput(inputUri)(Map(Column("info:from") -> 'fromColumn, Column("info:to") -> 'toColumn))


  topCorresponders.write(Tsv(outputUri + sep + "top-correspondents-enron"))
}
