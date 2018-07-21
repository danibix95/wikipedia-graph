package it.unitn.bdsn.bissoli.daniele

import java.sql.Timestamp

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{collect_set, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}

object Similarity extends Serializable {
  // import implicits using current spark session
  // (used to recognize $ as col and for default encoders)
  lazy private val spark = SparkSession.getActiveSession.get
  import spark.implicits._

  /** Returns the dot product of given two arrays.
    * */
  private def dot(v1: Seq[Double], v2: Seq[Double]) : Double = {
    require(v1.length == v2.length,
      s"Vector dimensions do not match: Dim(v1)=${v1.length}"
        + s"and Dim(v2)=${v2.length}."
    )
    // take each elements of two lists, multiply together
    // and sum the resulting list
    ((v1 zip v2) map { case (a, b) => a * b }).sum
  }

  /** Returns a DataFrame with the cosine similarity of each pair of pages
    * given in input as DataFrame containing Wikipedia pages representation.
    * */
  private def computeCS(df1 : DataFrame, df2 : DataFrame) : DataFrame = {
    // UDF function to check if the title of second page
    // is contained in one of the links of the first one
    val isLinked = udf {
      (links: Seq[String], title: String) => links.contains(title)
    }

    df1.as("A").join(
        df2.as("B"),
        $"A.title" =!= $"B.title"
        && isLinked($"A.neighbours", $"B.title")
        && $"A.timestamp" >= $"B.timestamp"
      )
      .select(
        $"A.title".as('title_a),
        $"B.title".as('title_b),
        $"A.timestamp".as('timestamp_a),
        $"B.timestamp".as('timestamp_b),
        $"A.features".as('features_a),
        $"B.features".as('features_b),
        $"A.norm".as('norm_a),
        $"B.norm".as('norm_b)
      )
      .map(r => {
        val t1 = r.getAs[String]("title_a")
        val t2 = r.getAs[String]("title_b")
        val ts1 = r.getAs[Timestamp]("timestamp_a")
        val ts2 = r.getAs[Timestamp]("timestamp_b")
        val s1 = r.getAs[Vector]("features_a").toArray
        val s2 = r.getAs[Vector]("features_b").toArray
        val n1 = r.getAs[Double]("norm_a")
        val n2 = r.getAs[Double]("norm_b")

        // compute cosine similarity between two features vectors
        val cs = dot(s1, s2) / (n1 * n2)
        // return a new Row with cosine similarity appended at the end
        (t1, ts1, t2, ts2, cs)
      })
      .toDF(
        "title_1",
        "timestamp_1",
        "title_2",
        "timestamp_2",
        "cosine_similarity"
      )
  }

  def compare(df: DataFrame, inputPath: String) : DataFrame = {
    val linksFlatten = udf {
      links: Seq[Seq[String]] => links.map(_.toSet)
        .reduce((a, b) => a | b).toSeq
    }
    // collect the set of all linked pages by all the page versions
    val neighbours : DataFrame = df.groupBy("title")
      .agg(collect_set("neighbours").alias("tmp"))
      .withColumn("all_neighbours", linksFlatten($"tmp"))
      .select("title", "all_neighbours")
      // taking the first row of resulting dataframe is correct
      // since each input dataframe was built to contain a single page
      .take(1)(0)
      .getAs[Seq[String]]("all_neighbours")
      .map(l => Try(spark.read.parquet(s"$inputPath/to_split=$l")))
      .collect { case Success(readDF) => readDF }
      .filter(_.count() > 0) match {
        case list if list.length > 1 => list.reduce(_.union(_)).distinct()
        case single if single.length == 1 => single.head
        case _ => Seq.empty[(String, Timestamp, Vector, Double, Seq[String])]
          .toDF("title", "timestamp", "features", "norm", "neighbours")
      }

    computeCS(df, neighbours)
  }
}
