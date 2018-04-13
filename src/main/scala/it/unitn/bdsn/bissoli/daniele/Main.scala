package it.unitn.bdsn.bissoli.daniele

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

import java.sql.Timestamp
import java.util.TimeZone

import scala.sys

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
    import spark.implicits._

    // retrieve the path of resources folder
    val resourcesDir = sys.env.getOrElse("SP_RES_DIR", "")

    val fp = s"$resourcesDir/NGC_4457.xml"
//    val fp = s"$resourcesDir/small.xml"
    val df = extractPages(spark, fp)

    // here you'll need to apply the preprocessing operations
//    println(PageParser.extractFeatures(df.head().getAs[String]("text")).toString)

    /* Pre-processing of Wikipedia pages */
    val df3 = df.map(r => {
      val (infobox, linksContext) =
        PageParser.extractFeatures(r.getAs[String]("text"))
      // build new dataframe row
      (
        r.getAs[String]("title"),
        r.getAs[Timestamp]("timestamp"),
        infobox,
        linksContext
      )
    }).toDF("title", "timestamp", "infobox", "links")

    df3.show()

    // TODO: !!! split infobox extraction from links context extraction!!!
    /* 2 approaches:
      - split infobox vector from links vector and then combine obtained similarities in some way later
      - average infobox and links vectors and then compute similarity
        (caveaut => single sim, but all the pages without an infobox will have something in common (average with 0)
    */

    // Input data: Each row is a bag of words from a sentence or document.
//    val documentDF = spark.createDataFrame(Seq(
//      ("hello", "2018-03-27T12:25:58Z", "Hi I heard about Spark".split(" ")),
//      ("class", "2018-03-27T11:25:58","I wish Java could use case classes".split(" ")),
//      ("log1", "2018-03-26T12:25:58","Logistic regression models are neat".split(" ")),
//      ("log2", "2017-03-27T12:25:58","Logistic regression models are good".split(" "))
//    )
//      .map(e => (e._1, e._2, e._3)))
//      .toDF("title", "ts","text")
//      .withColumn("timestamp",to_utc_timestamp(col("ts"),TimeZone.getDefault.getID))

//    val cs = new CosineSimilarity("infobox", 512)
//    val result : DataFrame = cs.computeCS(df3)
//
//    result.write.csv(s"$resourcesDir/small")

//    val cs = new CosineSimilarity("text", 16)
//    val result : DataFrame = cs.computeCS(documentDF)

//    result.explain()
//    result.show(50)

    spark.stop()
  }

  /** Returns a Dataframe representing all Wikipedia pages
    * with their edit history contained in the given file.
    * It expect an input file complaint to XML schema provided
    * here: https://www.mediawiki.org/xml/export-0.8.xsd
    * */
  def extractPages(spark : SparkSession, filePath : String) : DataFrame = {
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
      )
  }
}
