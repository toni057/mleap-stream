package com.tb.data

import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}
import ml.combust.mleap.tensor.Tensor


case class Iris(sepal_length: Double, sepal_width: Double, petal_length: Double, petal_width: Double) {

  def convertIrisToLeapFrame =
    for {
      schema <- Iris.getSchema
      row = Row(petal_length, petal_width, sepal_length, sepal_width)
    } yield DefaultLeapFrame(schema, Seq(row))

  def score(transformer: Transformer) = {
    for {
      example <- convertIrisToLeapFrame
      scored <- transformer.transform(example)
      scores <- scored.select("probability")
      probabilityRow = scores.collect().head
      probabilityTensor = probabilityRow.getAs[Tensor[Double]](0)
      probabilities = probabilityTensor.toArray
    } yield probabilities
  }
}

object Iris {

  def getSchema = StructType(
    Seq(
      StructField("petal_length", ScalarType.Double),
      StructField("petal_width", ScalarType.Double),
      StructField("sepal_length", ScalarType.Double),
      StructField("sepal_width", ScalarType.Double)
    )
  )

}