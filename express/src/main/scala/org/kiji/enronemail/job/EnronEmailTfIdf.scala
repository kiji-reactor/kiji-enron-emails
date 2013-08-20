package org.kiji.enronemail.job

import com.twitter.scalding.{Tsv, Args}
import org.kiji.express.flow.{Column, KijiInput, KijiJob}
import org.slf4j.LoggerFactory
import com.twitter.scalding.mathematics.Matrix
import org.kiji.express.{EntityId, KijiSlice}

class EnronEmailTfIdf(args: Args) extends KijiJob(args) {

  import Matrix._

  lazy val log = LoggerFactory.getLogger(getClass)

  // Conveniently get the correct path separator for the platform we're running on
  val sep = java.io.File.separatorChar


  // Parse arguments
  val inputUri: String = args("input")
  val outputUri: String = args("output")

  /**
   *                             word1   word2  ... wordN
   * Create a matrix of email1 [ count1, count2 ... countN ]
   *                    email2 [ count1, count2 ... countN ]
   *                    ...
   *                    emailN [ count1, count2 ... countN ]
   */
  val docWordMatrix =
  KijiInput(inputUri)(Map(Column("info:body") -> 'docColumn))
  .map('entityId -> 'entityId) { entityId: EntityId => entityId.toString() }
  .flatMap('docColumn -> 'doc) { column: KijiSlice[String] => column.cells.map(cell => cell.datum) }
  // TODO Fill me in

  /**
   * Compute the document frequency of each row
   *
   * Example Output:
   * enron	2.0
   * ensure	1.0
   * entail	1.0
   */
  // TODO Fill me in

  /**
   * Compute the inverse document frequency vector
   *
   * Example Output:
   * 1	follow	9.603626344986191
   * 1	formed	9.603626344986191
   * 1	forms	  9.603626344986191
   */
  // TODO Fill me in

  /**
   * Zip the row vector along the entire document - word matrix
   * This essentially joins the inverse document frequency vector to the term frequency matrix and
   * then discards the term frequency.
   *
   * Example Output:
   * EntityId(rob.bradley@enron.com,995048400000)	piece	9.603626344986191
   * EntityId(rob.bradley@enron.com,995048400000)	play	9.603626344986191
   * EntityId(rob.bradley@enron.com,995048400000)	point	9.603626344986191
   */
  // TODO Fill me in


  // Write the final result
  .write(Tsv(outputUri + sep + "tf-idf-matrix"))

  def log2(x : Double) = scala.math.log(x)/scala.math.log(2.0)

}
