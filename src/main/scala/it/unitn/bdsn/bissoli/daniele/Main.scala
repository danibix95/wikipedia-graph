package it.unitn.bdsn.bissoli.daniele

import java.io.File
import java.util.TimeZone

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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
    var inputFolder = ""
    if (args.length == 2) {
      // the root filePath where required resources are located
      resourcesDir = args(0)
      // which sub-folder of input folder has to be loaded
      inputFolder = args(1)
    }
    else {
      spark.stop()
    }

    val inputs = s"input/$inputFolder"
    val outputDir = s"output/$inputFolder"

    preprocessing(resourcesDir, inputs, outputDir)

    comparing(s"$resourcesDir/$outputDir")

    filtering(s"$resourcesDir/$outputDir")

    spark.stop()
  }

  def preprocessing(path: String, folder: String, output: String)
    : Unit = {
    import spark.implicits._

    val wikiW2V = Word2VecModel.load(s"$path/W2V")

    getListOfFiles(s"$path/$folder")
      .map(extractPages)
      // Pre-processing of Wikipedia pages
      .map(PagePreprocessor.preprocess(_, wikiW2V))
      // copy the title column since partitioning remove selected column
      .map(_.withColumn("to_split", $"title"))
      .reduceLeft(_.union(_))
      .write.partitionBy("to_split")
      .mode(SaveMode.Overwrite).parquet(s"$path/$output/preprocessed")
  }

  def comparing(path: String, partitions: Int = 32): Unit = {
    val inputPath = s"$path/preprocessed"

    getListOfSubDirectories(inputPath)
      .map(spark.read.parquet(_))
      .map(Similarity.compare(_, inputPath))
      .reduceLeft(_.union(_))
      .distinct
      .coalesce(partitions)
      .write.mode(SaveMode.Overwrite)
      .parquet(raw"$path/relationships/")
  }

  def filtering(resourcePath: String, partitions: Int = 32) : Unit = {
    val inputDir = s"$resourcePath/relationships"
    val outputDir = s"$resourcePath/final"

    DataFilter.filter(spark.read.parquet(inputDir))
      .coalesce(partitions)
      .write.mode(SaveMode.Overwrite).csv(outputDir)
  }

  def getListOfFiles(directoryName: String): Array[String] = {
    val path = new Path(directoryName)
    path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      .listStatus(path)
      .filter(_.isFile)
      .map(_.getPath.toString)
  }

  def getListOfSubDirectories(directoryName: String): Array[String] = {
    val path = new Path(directoryName)
    path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      .listStatus(path)
      .filter(_.isDirectory)
      .map(_.getPath.toString)
  }

  /** Returns a Dataframe representing all Wikipedia pages
    * with their edit history contained in the given file.
    * It expect an input file complaint to XML schema provided
    * here: https://www.mediawiki.org/xml/export-0.8.xsd
    * */
  def extractPages(file : String) : DataFrame = {
    val zip = udf((xs: Seq[String], ys: Seq[String]) => xs.zip(ys))

    // the following is the structure of input XML file
    // by default all the values are nullable
    val redirectType = StructType(
      List(
        StructField("_VALUE", StringType),
        StructField("_title", StringType)
      )
    )

    val contributorType = StructType(
      List(
        StructField("_VALUE", StringType),
        StructField("_deleted", StringType),
        StructField("username", StringType),
        StructField("ip", StringType),
        StructField("id", LongType)
      )
    )

    val commentType = StructType(
      List(
        StructField("_VALUE", StringType),
        StructField("_deleted", StringType)
      )
    )

    val contentModelType = StructType(
      List(
        StructField("_VALUE", StringType)
      )
    )

    val contentFormatType = StructType(
      List(
        StructField("_VALUE", StringType)
      )
    )

    val textType = StructType(
      List(
        StructField("_VALUE", StringType),
        StructField("_bytes", LongType),
        StructField("_deleted", StringType),
        StructField("_space", StringType),
        StructField("id", StringType)
      )
    )

    val revisionType = StructType(
      List(
        StructField("id", LongType),
        StructField("parentid", LongType),
        StructField("timestamp", StringType),
        StructField("contributor", contributorType),
        StructField("minor", StringType),
        StructField("comment", commentType),
        StructField("model", contentModelType),
        StructField("format", contentFormatType),
        StructField("text", textType),
        StructField("sha1", StringType)
      )
    )

    val discussionType = StructType(
      List(
          StructField("ThreadSubject", StringType),
          StructField("ThreadParent", LongType),
          StructField("ThreadAncestor", LongType),
          StructField("ThreadPage", StringType),
          StructField("ThreadID", LongType),
          StructField("ThreadAuthor", StringType),
          StructField("ThreadEditStatus", StringType),
          StructField("ThreadType", StringType)
      )
    )

    val pageSchema = StructType(
      List(
        StructField("title", StringType),
        StructField("ns", LongType),
        StructField("id", LongType),
        StructField("redirect", redirectType),
        StructField("restrictions", StringType),
        StructField("revision", ArrayType(revisionType, containsNull = true)),
        StructField("discussionthreadinginfo", discussionType)
      )
    )

    spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .schema(pageSchema)
      .load(file)
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
      .na.drop(Seq("text"))
  }
}
