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

  // compute the overall document frequency of each row
  val docFreq = docWordMatrix.binarizeAs[Double].sumRowVectors

  // compute the inverse document frequency vector
  val invDocFreqVct = docFreq.toMatrix(1).rowL1Normalize.mapValues( x => log2(1/x) )

  // zip the row vector along the entire document - word matrix
  val invDocFreqMat = docWordMatrix.zip(invDocFreqVct.getRow(1)).mapValues( pair => pair._2 )

  // multiply the term frequency with the inverse document frequency and keep the top nrWords
  docWordMatrix.hProd(invDocFreqMat).topRowElems(10)

  .write(Tsv(outputUri + sep + "doc-word-matrix"))

  def log2(x : Double) = scala.math.log(x)/scala.math.log(2.0)

}
