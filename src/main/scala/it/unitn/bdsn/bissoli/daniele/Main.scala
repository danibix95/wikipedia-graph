package it.unitn.bdsn.bissoli.daniele

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
    // !! Important to put here since only here the spark object
    // is created with its properties
//    import spark.implicits._

    val resourcesDir = "file:///home/daniele/university/04_computerScience/BDSN/project/wikipedia-graph/src/main/resources"
//    val resourcesDir = "file:///home/daniele/university/04_computerScience/BDSN/project/sample_data"
    // load into memory the xml file as a dataframe which rows are each page of
//    val fp = s"$resourcesDir/medium.xml"


    val fp = s"$resourcesDir/NGC_4457.xml"
    val df = extractPages(spark, fp)
    df.printSchema()
    df.filter(col("title").equalTo("NGC 4457")).show()
    df.show(300)

//    df.foreach((r) => { println(extractInfobox(r)) })
    println(PageAnalyzier.extractInfobox(df.head()))

    // TODO:  SIMILARITY BETWEEN PAGES => check internal wikipedia links

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      ("hello", "Hi I heard about Spark".split(" ")),
      ("class","I wish Java could use case classes".split(" ")),
      ("log1","Logistic regression models are neat".split(" ")),
      ("log2","Logistic regression models are good".split(" "))
    ).map(e => (e._1, e._2))).toDF("title","text")

    val cs = new CosineSimilarity("text", 10)
    val result : DataFrame = cs.computeCS(documentDF)

    result.explain()
    result.show()

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
      .select(
        col("title"),
        col("tmp._1").alias("timestamp"),
        col("tmp._2").alias("text")
      )
  }
}
