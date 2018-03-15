package it.unitn.bdsn.bissoli.daniele

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, udf}

import scala.util.matching.Regex

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
    val resourcesDir = "file:///home/daniele/university/04_computerScience/BDSN/project/wikipedia-graph/src/main/resources"
//    val resourcesDir = "file:///home/daniele/university/04_computerScience/BDSN/project/sample_data"
    // load into memory the xml file as a dataframe which rows are each page of
//    val fp = s"$resourcesDir/medium.xml"
    val fp = s"$resourcesDir/NGC_4457.xml"
    val df = extractPages(spark, fp)
    df.printSchema()
    df.filter(col("title").equalTo("NGC 4457")).show()
    df.show(300)

    println(extractInfobox(df.head()))

    // TODO:  SIMILARITY BETWEEN PAGES => check internal wikipedia links

    spark.stop()
  }

  /** Returns a Dataframe representing all Wikipedia pages
    * with their edit history contained in the given file.
    * It expect an input file complaint to XML schema provided
    * here: https://www.mediawiki.org/xml/export-0.8.xsd
    * */
  def extractPages(sparkSession: SparkSession, filePath: String) : DataFrame = {
    val zip = udf((xs: Seq[String], ys: Seq[String]) => xs.zip(ys))

    sparkSession.read
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
      .select(
        col("title"),
        col("tmp._1").alias("timestamp"),
        col("tmp._2").alias("text")
      )
  }

  def extractInfobox(page: Row) = {
    val infoboxFilter: Regex = """\{\{Infobox \w*\n(\s*\|.*)+""".r
    infoboxFilter.findFirstIn(page.getAs[String]("text"))
  }
}
