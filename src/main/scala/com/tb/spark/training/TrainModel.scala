package com.tb.spark.training

import com.tb.data.Iris
import com.tb.mleap.MleapUtils
import grizzled.slf4j.Logging
import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport.SparkTransformerOps
import ml.combust.mleap.tensor.Tensor
import resource.managed
import org.apache.spark.SparkConf
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}

import scala.util.Success


object TrainModel extends Logging {

  /**
   *
   * @param args - it should have two input parameters specified:
   *             --input-path src/main/resources/iris.json
   *             --mleap-path src/main/resources/logreg.zip
   */
  def main(args: Array[String]): Unit = {

    logger.info("Starting training program.")

    val argPar = parseInputArgs(args.toList)
    logger.info(argPar)

    implicit val spark: SparkSession = getSparkSession

    val df = argPar
      .get("input-path")
      .map(readInput)
      .getOrElse(throw new Exception("Please specify input data path via --input-path"))
      .transform(transformLabel)

    argPar
      .get("mleap-path")
      .map { path =>
        val pipelineModel = fitPipelineModel(df)
        serializeToMleap(df, pipelineModel, path)
      }
      .orElse(throw new Exception("Please specify mleap bundle file path via --mleap-path"))

    spark.stop()

    logger.info(
      loadAndScoreExample(argPar("mleap-path"))
        .map(_.mkString(" "))
    )
  }


  def parseInputArgs(args: List[String]): Map[String, String] =
    args match {
      case "--input-path" :: path :: tail => parseInputArgs(tail) + ("input-path" -> path)
      case "--mleap-path" :: path :: tail => parseInputArgs(tail) + ("mleap-path" -> path)
      case _ :: tail => parseInputArgs(tail)
      case Nil => Map.empty[String, String]
    }

  def getSparkSession: SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName("train-job")
      .set("spark.master", "local[2]")
      .set("spark.sql.shuffle.partitions", "1")
//      .set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=src/main/resources/log4j.properties")

    SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
  }

  def readInput(path: String)(implicit spark: SparkSession): DataFrame = {
    logger.info("Reading input.")
    spark.read.json(path)
  }

  def transformLabel(df: DataFrame): DataFrame = {
    logger.info("Label transformer.")
    val labelIndexer = new StringIndexer()
      .setInputCol("species")
      .setOutputCol("indexedSpecies")
      .setStringOrderType("alphabetAsc")
      .fit(df)

    labelIndexer.transform(df)
  }

  def buildPreProPipeline(df: DataFrame): Pipeline = {
    logger.info("Building prepro pipeline.")
    val vectorAssembler = new VectorAssembler()
      .setInputCols(df.columns.take(4))
      .setOutputCol("features")

    val standardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)

    new Pipeline()
      .setStages(Array(vectorAssembler, standardScaler))
  }

  def buildLogisticRegression: LogisticRegression = {
    logger.info("Building logistic regression.")
    new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("indexedSpecies")
      .setFitIntercept(true)
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
  }

  def buildFinalPipeline(df: DataFrame): Pipeline = {
    logger.info("Building final pipeline.")
    val prePro = buildPreProPipeline(df)
    val lr = buildLogisticRegression

    new Pipeline()
      .setStages(Array(prePro, lr))
  }

  def fitPipelineModel(df: DataFrame): PipelineModel = {
    logger.info("Training the model.")
    val pipeline = buildFinalPipeline(df)

    pipeline.fit(df)
  }

  def serializeToMleap(df: DataFrame, pipelineModel: PipelineModel, path: String): Unit = {
    logger.info(s"Serializing the bundle to ${fullPath(path)}.")
    val sbc = SparkBundleContext().withDataset(pipelineModel.transform(df))
    for (bf <- managed(BundleFile(fullPath(path)))) {
      pipelineModel.writeBundle.save(bf)(sbc).get
    }
  }

  private def fullPath(path: String) = s"jar:file:${System.getProperty("user.dir")}/$path"

  def loadAndScoreExample(path: String): Either[Throwable, Array[Double]] = {
    val scores = for {
      example <- Success(Iris(0.1, 0.2, 0.3, 0.4))
      transformer <- MleapUtils.loadMleapModel(path)
      probabilities <- example.score(transformer)
    } yield probabilities

    scores.toEither
  }

}