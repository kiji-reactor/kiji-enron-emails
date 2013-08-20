package org.kiji.enronemail.job

import com.twitter.scalding.{Tsv, Args}
import org.kiji.express.flow.{Column, KijiInput, KijiJob}
import org.slf4j.LoggerFactory
import com.twitter.scalding.mathematics.Matrix
import org.kiji.express.{EntityId, KijiSlice}

class TfIdfJob(args: Args) extends KijiJob(args) {

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
  .flatMap('doc -> 'word) {
    doc: String => doc.split("\\s+").map( _.trim.toLowerCase)
      // Filter stop words and short words.
      .filterNot(word => Constants.englishStopWords.contains(word) || word.length < 3) }
  .groupBy('entityId, 'word) { _.size('count) }
  .project('entityId, 'word, 'count)
  .toMatrix[String, String, Double]('entityId, 'word, 'count)

  /**
   * Compute the document frequency of each row
   *
   * Example Output:
   * enron	2.0
   * ensure	1.0
   * entail	1.0
   */
  val docFreq = docWordMatrix.binarizeAs[Double].sumRowVectors

  /**
   * Compute the inverse document frequency vector
   *
   * Example Output:
   * 1	follow	9.603626344986191
   * 1	formed	9.603626344986191
   * 1	forms	  9.603626344986191
   */
  val invDocFreqVct = docFreq.toMatrix(1).rowL1Normalize.mapValues( x => log2(1/x) )

  /**
   * Zip the row vector along the entire document - word matrix
   * This essentially joins the inverse document frequency vector to the term frequency matrix and
   * then discards the term frequency.
   *
   * Example Output:
   * EntityId(rob.bradley@enron.com,995048400000)	piece	9.603626344986191
   * EntityId(rob.bradley@enron.com,995048400000)	play	9.603626344986191
   * EntityId(rob.bradley@enron.com,995048400000)	point	9.603626344986191
   */                                                          // Effectively element-wise multiply of the two matrices
  val invDocFreqMat = docWordMatrix.zip(invDocFreqVct.getRow(1)).mapValues( pair => pair._1 * pair._2 )

  //  docWordMatrix.hProd(invDocFreqMat).topRowElems(10)
  .topRowElems(10) // Save only the top ten highest scoring words
  .write(Tsv(outputUri + sep + "tf-idf-matrix"))

  def log2(x : Double) = scala.math.log(x)/scala.math.log(2.0)

}
