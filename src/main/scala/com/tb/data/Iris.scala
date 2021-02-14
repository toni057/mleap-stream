package com.tb.data

import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

case class Iris(sepalLength: Double, sepalWidth: Double, petalLength: Double, petalWidth: Double)

object Iris {

  def convertIrisToLeapFrame(iris: Iris) =
    for {
      schema <- getSchema
      row = Row(iris.petalLength, iris.petalWidth, iris.sepalLength, iris.sepalWidth)
    } yield DefaultLeapFrame(schema, Seq(row))

  def getSchema = StructType(
    Seq(
      StructField("petal_length", ScalarType.Double),
      StructField("petal_width", ScalarType.Double),
      StructField("sepal_length", ScalarType.Double),
      StructField("sepal_width", ScalarType.Double)
    )
  )

}