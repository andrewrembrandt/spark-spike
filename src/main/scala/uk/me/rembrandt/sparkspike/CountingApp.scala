package uk.me.rembrandt.sparkspike

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object CountingLocalApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))
  val session = SparkSession
    .builder()
    .master("local")
    .appName("SparkSpike")
    .getOrCreate()

  Runner.run(session, inputFile, outputFile)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object CountingApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  val session = SparkSession.builder().getOrCreate()
  Runner.run(session, inputFile, outputFile)
}

object Runner {
  def run(session: SparkSession, inputFile: String, outputFile: String): Unit = {
    val sc = session.sparkContext

    val rdd = sc.textFile(inputFile)
    val counts = WordCount.withStopWordsFiltered(rdd)
    counts.saveAsTextFile(outputFile)
  }
}
