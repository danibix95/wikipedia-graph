package it.unitn.bdsn.bissoli.daniele


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import java.sql.Timestamp
import java.util.TimeZone

import scala.sys.env

trait SparkSessionWrapper {
  // remove master if you want to execute this program in a cluster
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[4]")
      .appName("wikipediaGraph")
      .getOrCreate()
  }
}

object Main extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {

    // retrieve the path of resources folder
    val resourcesDir = env.getOrElse("SP_RES_DIR", "")

    //    val fp = s"$resourcesDir/test.xml"
//    val fp = s"$resourcesDir/NGC_4457_neighbours.xml"
    //    val fp = s"$resourcesDir/NGC_4457.xml"
    intermediate(resourcesDir, "t_tiny.xml")

//    println(getListOfSubDirectories(s"$resourcesDir/preprocessed").mkString("\n"))
//    val folders : Seq[String] = getListOfSubDirectories(s"$resourcesDir/preprocessed").take(4)
//    folders.map((l: String) => spark.read.parquet(s"$resourcesDir/preprocessed/$l"))
//           .foreach(_.show(5))



//    val cs = new CosineSimilarity("infobox", "links", "neighbours", 1024)
//    val result : DataFrame = cs.computeCS(df3)
//    result.explain()

    spark.stop()
  }

  def intermediate(path: String, resource: String) : Unit = {
    import spark.implicits._

    val df = extractPages(s"$path/$resource")

    /* Pre-processing of Wikipedia pages */
    val preprocessedDF = df.map(r => {
      val (infobox, neighbours, linksContext) =
        PagePreprocessor.extractFeatures(r.getAs[String]("text"))
      // build new dataframe row
      (
        r.getAs[String]("title"),
        r.getAs[Timestamp]("timestamp"),
        infobox,
        neighbours,
        linksContext
      )
    }).toDF("title", "timestamp", "infobox", "neighbours", "links")

    PagePreprocessor
      .computeFeaturesVectors(preprocessedDF, "infobox", "links", 300)
      /* copy the title column since partitioning remove the column */
      .withColumn("to_split", $"title")
      .write.partitionBy("to_split")
      .mode(SaveMode.Overwrite).parquet(s"$path/preprocessed")

    // remove from memory previous dataframes
    df.unpersist()
    preprocessedDF.unpersist()
  }

  def getListOfSubDirectories(directoryName: String): Array[String] = {
    new File(directoryName)
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName/*.stripPrefix("title=")*/)
  }

  /** Returns a Dataframe representing all Wikipedia pages
    * with their edit history contained in the given file.
    * It expect an input file complaint to XML schema provided
    * here: https://www.mediawiki.org/xml/export-0.8.xsd
    * */
  def extractPages(filePath : String) : DataFrame = {
    val zip = udf((xs: Seq[String], ys: Seq[String]) => xs.zip(ys))

    spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(filePath)
      .select(
        col("title"),
        col("revision.timestamp"),
        col("revision.text._VALUE").as("text")
      )
      /* convert pages edits sequence into a new row for each edit entry */
      .withColumn(
        "tmp",
        explode(zip(col("timestamp"), col("text")))
      )
      /* convert read timestamp from string to timestamp type */
      .withColumn("timestamp",
        to_utc_timestamp(col("tmp._1"),
          TimeZone.getDefault.getID)
      )
      .select(
        col("title"),
        col("timestamp"),
        col("tmp._2").alias("text")
      ).na.drop(Seq("text"))
  }
}
