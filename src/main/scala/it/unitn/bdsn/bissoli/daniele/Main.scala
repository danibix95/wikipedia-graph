package it.unitn.bdsn.bissoli.daniele

import java.io.File
import java.sql.Timestamp
import java.util.TimeZone

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.util.{Success, Try}

trait SparkSessionWrapper {
  // remove master if you want to execute this program in a cluster
  lazy val spark: SparkSession = {
    val sp = SparkSession
      .builder()
      .appName("wikipediaGraph")

    if (new File("/home/daniele").exists()) {
      sp.master("local[*]")
    }

    sp.getOrCreate()
  }
}

object Main extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {

    var resourcesDir = ""
    var inputFile = ""
    if (args.length == 2) {
      resourcesDir = args(0)
      inputFile = args(1)
    }
    else {
      spark.stop()
    }

    // TODO: find a way to load (in memory) the data coming from a Wikipedia dump!

    val resource = s"input/$inputFile"
    val outputDir = "output/" ++ inputFile.stripSuffix(".xml")

    intermediate(resourcesDir, resource, outputDir)

    similarity(resourcesDir, outputDir)

    spark.stop()
  }

  def intermediate(path: String, resource: String, output: String) : Unit = {
    import spark.implicits._

    val df = extractPages(s"$path/$resource")

    /* Pre-processing of Wikipedia pages */
    val preprocessedDF = df.map(r => {
      val (processedPage, neighbours) =
        PagePreprocessor.extractFeatures(r.getAs[String]("text"))
      // build new dataframe row
      (
        r.getAs[String]("title"),
        r.getAs[Timestamp]("timestamp"),
        processedPage,
        neighbours
      )
    }).toDF("title", "timestamp", "pageProcessed", "neighbours")

    // load the Word2Vec model
    val W2VModel = Word2VecModel.load(s"$path/W2V")

    PagePreprocessor.computeFeaturesVectors(preprocessedDF, W2VModel, 300)
      /* copy the title column since partitioning remove the column */
      .withColumn("to_split", $"title")
      .write.partitionBy("to_split")
      .mode(SaveMode.Overwrite).parquet(s"$path/$output/preprocessed")

    // remove from memory previous dataframes
    df.unpersist()
    preprocessedDF.unpersist()
  }

  def getListOfSubDirectories(directoryName: String): Array[String] = {
    val path = new Path(directoryName)
    path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      .listStatus(path)
      .filter(_.isDirectory)
      .map(_.getPath.toString)
  }

  def similarity(path: String, output: String): Unit = {
    import spark.implicits._

    val linksFlatten = udf {
      links: Seq[Seq[String]] => links.map(_.toSet).reduce((a, b) => a | b).toSeq
    }
    val inputPath = s"$path/$output/preprocessed"

    val folders : Seq[String] = getListOfSubDirectories(inputPath)
    folders.map((l: String) => spark.read.parquet(l))
      .foreach((df : DataFrame) => {
        val tmpNeighbours : Row = df.groupBy("title")
          .agg(collect_set("neighbours").alias("tmp"))
          .withColumn("all_neighbours", linksFlatten($"tmp"))
          .select("title", "all_neighbours")
          // taking the first row of resulting dataframe is correct
          // since each input dataframe was built to contain a single page
          .take(1)(0)

        val tmpDFlist = tmpNeighbours.getAs[Seq[String]]("all_neighbours")
          .map(l => Try(spark.read.parquet(s"$inputPath/to_split=$l")))
          /* Note: this collect is of Scala collections, not Spark SQL */
          .collect { case Success(readDF) => readDF }

        // if there are more than 2 non-empty dataframe
        // then collapse into a single one
        val neighbours : DataFrame = {
          if (tmpDFlist.count(_.count() > 1) > 1) {
            tmpDFlist.reduceLeft(_.union(_)).distinct()
          }
          else {
            Try(tmpDFlist.filter(_.count() > 1).head) match {
              /* return the only dataframe in the list */
              case Success(first) => first
              /* otherwise return an empty dataframe */
              case _ => Seq.empty[(String, Timestamp, Vector, Double, Seq[String])]
                .toDF("title", "timestamp", "features", "norm", "neighbours")
            }
          }
        }

        val pageTitle = tmpNeighbours.getAs[String]("title")
          .stripPrefix("to_split=")
          .replaceAll("\\s", "")

        CosineSimilarity.computeCS(df, neighbours)
          .write.mode(SaveMode.Overwrite)
          .csv(raw"$path/$output/relationships/$pageTitle")

        neighbours.unpersist()
      })
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
