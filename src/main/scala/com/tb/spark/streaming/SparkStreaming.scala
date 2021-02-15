package com.tb.spark.streaming

import com.tb.data.Iris
import com.tb.mleap.MleapUtils
import com.tb.spark.training.TrainModel.{getSparkSession, parseInputArgs}

import grizzled.slf4j.Logging
import ml.combust.mleap.runtime.frame.Transformer
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.util.Try


object SparkStreaming extends Logging {


  /**
   *
   * @param args - it should have two input parameters specified:
   *             --input-path src/main/resources/stream/iris*.json
   *             --mleap-path src/main/resources/logreg.zip
   */
  def main(args: Array[String]): Unit = {

    logger.info("Starting spark serving program.")

    val argPar = parseInputArgs(args.toList)
    logger.info(argPar)

    implicit val spark: SparkSession = getSparkSession
    import spark.implicits._

    spark.streams.addListener(new LogPrinter())

        val streamingQuery = for {
          sourcePath <- Try(argPar("input-path"))
          source <- Try(getSource(sourcePath))
          transformerPath <- Try(argPar("mleap-path"))
          transformer <- MleapUtils.loadMleapModel(transformerPath)
          outputDS = source.transform(scoreIncomingDataset(transformer))
          streamingQuery = addConsoleSink[Array[Double]](outputDS)
        } yield streamingQuery

    streamingQuery.map(_.awaitTermination())
  }

  def getSource(path: String)(implicit spark: SparkSession) = {
    import spark.implicits._

    spark
      .readStream
      .option("maxFilesPerTrigger", "10")
      .schema(getSchema)
      .json(path)
      .drop("species")
      .as[Iris]
  }

  def getSchema = {
    StructType(
      List(
        StructField("sepal_length", DoubleType, true),
        StructField("sepal_width", DoubleType, true),
        StructField("petal_length", DoubleType, true),
        StructField("petal_width", DoubleType, true),
        StructField("species", StringType, true)
      )
    )
  }

  def scoreIncomingDataset(transformer: Transformer)(df: Dataset[Iris]) = {
    import df.sparkSession.implicits._
    df
      .flatMap(_.score(transformer).toOption)
  }

  def addConsoleSink[T](d: Dataset[T]) = {
    d.writeStream
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .outputMode("update")
      .format("console")
      .option("truncate", false)
      .start()
  }

  class LogPrinter extends StreamingQueryListener with Logging {

    override def onQueryProgress(event: QueryProgressEvent): Unit = {
      val duration = event.progress.durationMs.get("triggerExecution").toDouble / 1000.0
      logger.info(s"Duration is $duration.")
    }

    override def onQueryStarted(event: QueryStartedEvent): Unit = {
      println("Starting the stream job.")
    }

    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      logger.info("Terminating streaming job.")
    }

  }

}
